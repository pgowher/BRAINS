"""Replay smart-meter erasure closeouts on a disposable four-validator Besu chain."""
import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import heapq
import itertools
import json
import math
import os
from pathlib import Path
import random
from statistics import median
import subprocess
import time

from eth_abi import encode
from eth_utils import keccak

from qbft import BLOCK_SECONDS, CHAIN_ID, JAVA_LIMITS, RpcError, launch_network


ROOT = Path(__file__).resolve().parent
CONTRACT = ROOT / "contracts/ErasureCloseout.sol"
FIXTURES = ROOT / "fixtures/gridweave.json"
CLOSE_TOPIC = "0x" + keccak(text="CloseEvaluated(bytes32,uint8)").hex()
STATUS = ("UNKNOWN", "READY", "CLOSED", "DISPUTED")
DECISION = ("RECORDED", "BLOCKED", "TOO_EARLY")
BASE_CLOSE_MS = 750
OFFSETS_MS = (-650, -250, -80, 0, 80, 250, 600, None)


@dataclass(frozen=True)
class Mode:
    name: str
    guarded: bool
    grace_blocks: int = 0


@dataclass
class Actor:
    account: object
    peer: object
    nonce: int = 0

    def prepare(self, data, to=None, *, nonce=None):
        transaction = {
            "chainId": CHAIN_ID, "nonce": self.nonce if nonce is None else nonce,
            "gas": 1_000_000 if to is None else 150_000,
            "gasPrice": 0, "data": data,
        }
        if to is not None:
            transaction["to"] = bytes.fromhex(to.removeprefix("0x"))
        raw = self.account.sign_transaction(transaction).raw_transaction
        return "0x" + bytes(raw).hex()

    def submit(self, raw):
        submitted = time.monotonic_ns()
        tx_hash = self.peer.call("eth_sendRawTransaction", raw)
        acknowledged = time.monotonic_ns()
        self.nonce += 1
        return Transaction(tx_hash, submitted, acknowledged)

    def send(self, data, to=None):
        return self.submit(self.prepare(data, to))

    def submit_group(self, raw_transactions):
        if len(raw_transactions) == 1:
            return [self.submit(raw_transactions[0])]
        submitted = time.monotonic_ns()
        hashes = self.peer.batch([("eth_sendRawTransaction", raw) for raw in raw_transactions])
        acknowledged = time.monotonic_ns()
        self.nonce += len(hashes)
        return [Transaction(tx_hash, submitted, acknowledged) for tx_hash in hashes]


@dataclass
class Transaction:
    tx_hash: str
    submitted_ns: int
    acknowledged_ns: int
    seen_ns: int | None = None
    receipt: dict | None = None

    def observe(self, peer):
        if self.receipt is None:
            receipt = peer.call("eth_getTransactionReceipt", self.tx_hash)
            if receipt is not None:
                self.seen_ns = time.monotonic_ns()
                self.receipt = receipt
        return self.receipt is not None

    @property
    def order(self):
        return tuple(int(self.receipt[key], 16) for key in ("blockNumber", "transactionIndex"))

    def trace(self, ready_seen_ns):
        return {
            "hash": self.tx_hash,
            "block": self.order[0],
            "block_hash": self.receipt["blockHash"],
            "index": self.order[1],
            "submit_ms": ms_since(self.submitted_ns, ready_seen_ns),
            "ack_ms": ms_since(self.acknowledged_ns, ready_seen_ns),
            "seen_ms": ms_since(self.seen_ns, ready_seen_ns),
            "gas": int(self.receipt["gasUsed"], 16),
        }


def ms_since(timestamp, start):
    return round((timestamp - start) / 1_000_000, 3)


def await_receipts(observer, transactions):
    pending = list(transactions)
    deadline = time.monotonic() + 20
    while pending and time.monotonic() < deadline:
        pending = [tx for tx in pending if not tx.observe(observer)]
        if pending:
            time.sleep(0.04)
    if pending:
        raise TimeoutError(f"{len(pending)} of {len(transactions)} transactions still pending")
    for tx in transactions:
        if int(tx.receipt["status"], 16) != 1:
            raise RuntimeError(f"transaction reverted: {tx.tx_hash}")


def wait_for_height(peer, block):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if int(peer.call("eth_blockNumber"), 16) >= block:
            return
        time.sleep(0.04)
    raise TimeoutError(f"observer did not reach block {block}")


def compile_contract(solc):
    output = subprocess.check_output([
        solc, "--evm-version", "berlin", "--optimize", "--combined-json", "bin,hashes", str(CONTRACT)
    ], text=True)
    (artifact,) = json.loads(output)["contracts"].values()
    return artifact


@dataclass
class Trial:
    key: bytes
    fixture: dict
    offset_ms: int | None = None
    ready: Transaction | None = None
    challenge: Transaction | None = None
    close: Transaction | None = None

    @property
    def evidence(self):
        return bytes.fromhex(self.fixture["evidence_sha256"])

    @property
    def adverse(self):
        return bytes.fromhex(self.fixture["adverse_stream_sha256"])


class Replay:
    def __init__(self, network, artifact):
        self.assessor = Actor(network.assessor, network.peers[0])
        self.challenger = Actor(network.challenger, network.peers[1])
        self.observer = network.peers[2]
        self.other_observer = network.peers[3]
        self.artifact = artifact
        self.address = None

    def calldata(self, method, *words):
        return "0x" + self.artifact["hashes"][method] + b"".join(words).hex()

    def deploy(self, mode):
        args = encode(["address", "bool", "uint64"],
                      [self.challenger.account.address, mode.guarded, mode.grace_blocks])
        tx = self.assessor.send("0x" + self.artifact["bin"] + args.hex())
        await_receipts(self.observer, [tx])
        self.address = tx.receipt["contractAddress"]

    def register(self, trials):
        for trial in trials:
            trial.ready = self.assessor.send(
                self.calldata("registerReady(bytes32,bytes32)", trial.key, trial.evidence), self.address
            )
        await_receipts(self.observer, [trial.ready for trial in trials])
        for trial in trials:
            if self.status(trial) != "READY":
                raise AssertionError("preflight must start from the registered ready case")

    def file_challenge(self, trial):
        return self.challenger.send(
            self.calldata("fileChallenge(bytes32,bytes32)", trial.key, trial.adverse), self.address
        )

    def record_close(self, trial):
        return self.assessor.send(
            self.calldata("recordClose(bytes32,bytes32)", trial.key, trial.evidence), self.address
        )

    def status(self, trial, peer=None, block="latest"):
        result = (peer or self.observer).call("eth_call", {
            "to": self.address, "data": self.calldata("statusOf(bytes32)", trial.key),
        }, block)
        return STATUS[int(result, 16)]

    def decision(self, trial, transaction):
        logs = [log for log in transaction.receipt["logs"]
                if log["topics"] == [CLOSE_TOPIC, "0x" + trial.key.hex()]]
        (log,) = logs
        return DECISION[int(log["data"], 16)]

    def row(self, trial, mode, workload, replicate, scenario=None):
        challenge, close = trial.challenge, trial.close
        decision = self.decision(trial, close)
        recorded = decision == "RECORDED"
        challenge_first = challenge is not None and challenge.order < close.order
        premature = (recorded and challenge is not None and
                     challenge.acknowledged_ns < close.submitted_ns and not challenge_first)
        last_tx = max([tx for tx in (challenge, close) if tx is not None], key=lambda tx: tx.order)
        block = hex(last_tx.order[0])
        wait_for_height(self.other_observer, last_tx.order[0])
        other_hash = self.other_observer.call("eth_getBlockByNumber", block, False)["hash"]
        if other_hash != last_tx.receipt["blockHash"]:
            raise AssertionError("observer block hashes disagree")
        status = self.status(trial, block=block)
        if status != self.status(trial, peer=self.other_observer, block=block):
            raise AssertionError("observers disagree on case status at the same block")
        expected = "DISPUTED" if challenge else "READY"
        if recorded and (challenge is None or challenge_first):
            expected = "CLOSED"
        if status != expected or (mode.guarded and challenge_first and recorded):
            raise AssertionError(f"inconsistent decision for {trial.key.hex()}")
        return {
            "mode": mode.name, "grace_blocks": mode.grace_blocks, "workload": workload,
            "replicate": replicate, "scenario": scenario, "offset_ms": trial.offset_ms,
            "case_id": trial.key.hex(), "fixture_pair": trial.fixture["pair_id"],
            "contract": self.address, "decision": decision, "final_status": status,
            "recorded_after_finalized_challenge": recorded and challenge_first,
            "recorded_while_earlier_challenge_pending": premature,
            "ready": trial.ready.trace(trial.ready.seen_ns),
            "close": close.trace(trial.ready.seen_ns),
            "challenge": challenge.trace(trial.ready.seen_ns) if challenge else None,
        }

    def pilot(self, mode, fixtures):
        trials = make_trials(fixtures, "pilot", 0, [None, None, None])
        uncontested, challenged_first, closed_first = trials
        self.register(trials)
        for address, calldata in (
            (self.assessor.account.address,
             self.calldata("fileChallenge(bytes32,bytes32)", uncontested.key, uncontested.adverse)),
            (self.assessor.account.address,
             self.calldata("recordClose(bytes32,bytes32)", uncontested.key, uncontested.adverse)),
        ):
            try:
                self.observer.call("eth_call", {"from": address, "to": self.address, "data": calldata}, "latest")
            except RpcError as error:
                if "revert" not in str(error).lower():
                    raise
            else:
                raise AssertionError("invalid authorization or evidence digest was accepted")
        if mode.grace_blocks >= 3:
            early = self.record_close(uncontested)
            await_receipts(self.observer, [early])
            if self.decision(uncontested, early) != "TOO_EARLY":
                raise AssertionError("the contract failed to enforce the challenge window")
        challenged_first.challenge = self.file_challenge(challenged_first)
        await_receipts(self.observer, [challenged_first.challenge])
        wait_for_height(self.observer, max(t.ready.order[0] for t in trials) + mode.grace_blocks)
        for trial in trials:
            trial.close = self.record_close(trial)
        await_receipts(self.observer, [trial.close for trial in trials])
        closed_first.challenge = self.file_challenge(closed_first)
        await_receipts(self.observer, [closed_first.challenge])
        scenarios = ["uncontested", "challenge-first", "close-first"]
        rows = [self.row(t, mode, "pilot", 0, s) for t, s in zip(trials, scenarios)]
        expected = ["RECORDED", "BLOCKED" if mode.guarded else "RECORDED", "RECORDED"]
        if [row["decision"] for row in rows] != expected:
            raise AssertionError("pilot produced an unexpected decision")
        return rows

    def timed_group(self, mode, workload, replicate, trials):
        self.register(trials)
        start = time.monotonic_ns()
        tie = itertools.count()
        scheduled = []
        pending = []
        for trial in trials:
            target = start + BASE_CLOSE_MS * 1_000_000
            heapq.heappush(scheduled, (target, next(tie), "close", trial))
            if trial.offset_ms is not None:
                challenge_at = target + trial.offset_ms * 1_000_000
                heapq.heappush(scheduled, (challenge_at, next(tie), "challenge", trial))
        deadline = start + (20 + mode.grace_blocks * BLOCK_SECONDS) * 1_000_000_000
        while scheduled or pending:
            now = time.monotonic_ns()
            if now > deadline:
                raise TimeoutError(f"unfinished {mode.name} {workload} group in replicate {replicate}")
            current_height = None
            while scheduled and scheduled[0][0] <= now:
                _, _, action, trial = heapq.heappop(scheduled)
                if action == "challenge":
                    trial.challenge = self.file_challenge(trial)
                    pending.append(trial.challenge)
                    continue
                if mode.grace_blocks and current_height is None:
                    current_height = int(self.observer.call("eth_blockNumber"), 16)
                # Observe eligibility before release so delay comes from policy, not retries.
                if mode.grace_blocks and current_height < trial.ready.order[0] + mode.grace_blocks - 1:
                    heapq.heappush(scheduled, (now + 40_000_000, next(tie), action, trial))
                    continue
                trial.close = self.record_close(trial)
                pending.append(trial.close)
            pending = [tx for tx in pending if not tx.observe(self.observer)]
            if scheduled or pending:
                time.sleep(0.025)
        completed = [tx for trial in trials for tx in (trial.close, trial.challenge) if tx is not None]
        await_receipts(self.observer, completed)
        return [self.row(t, mode, workload, replicate) for t in trials]


def make_trials(fixtures, workload, replicate, offsets):
    trials = []
    for index, offset in enumerate(offsets):
        fixture = fixtures[index % len(fixtures)]
        identity = f"{workload}/{replicate}/{index}/{fixture['pair_id']}"
        trials.append(Trial(hashlib.sha256(identity.encode()).digest(), fixture, offset))
    return trials


def trial_groups(fixtures, workload, replicate, seed, burst_width):
    offsets = list(OFFSETS_MS)
    if workload == "burst":
        offsets *= burst_width // len(offsets)
    random.Random(f"{seed}/{workload}/{replicate}").shuffle(offsets)
    trials = make_trials(fixtures, workload, replicate, offsets)
    width = burst_width if workload == "burst" else 1
    for start in range(0, len(trials), width):
        yield trials[start:start + width]


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["mode"], row["workload"])].append(row)
    summary = []
    for (mode, workload), group in groups.items():
        uncontested = sorted(row["close"]["seen_ms"] for row in group
                             if row["challenge"] is None and row["decision"] == "RECORDED")
        summary.append({
            "mode": mode, "workload": workload, "cases": len(group),
            "challenged": sum(row["challenge"] is not None for row in group),
            "recorded": sum(row["decision"] == "RECORDED" for row in group),
            "blocked": sum(row["decision"] == "BLOCKED" for row in group),
            "too_early": sum(row["decision"] == "TOO_EARLY" for row in group),
            "recorded_after_finalized_challenge": sum(row["recorded_after_finalized_challenge"] for row in group),
            "recorded_while_earlier_challenge_pending": sum(
                row["recorded_while_earlier_challenge_pending"] for row in group
            ),
            "uncontested_n": len(uncontested),
            "uncontested_median_ms": median(uncontested) if uncontested else None,
            "uncontested_p95_ms": uncontested[math.ceil(0.95 * len(uncontested)) - 1] if uncontested else None,
        })
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", choices=["pilot", "sweep"])
    parser.add_argument("--besu", default=os.getenv("BESU", str(ROOT / ".tools/besu-26.10.2/bin/besu")))
    parser.add_argument("--solc", default=os.getenv("SOLC", str(ROOT / ".tools/solc")))
    parser.add_argument("--out", type=Path)
    parser.add_argument("--seed", type=int, default=20261110)
    parser.add_argument("--replicates", type=int, default=1)
    parser.add_argument("--burst-width", type=int, default=24)
    parser.add_argument("--grace-blocks", type=int, default=3)
    args = parser.parse_args()
    if args.replicates < 1 or args.burst_width < 8 or args.burst_width % 8 or not 1 <= args.grace_blocks <= 10:
        parser.error("use a positive replicate count, a burst width divisible by 8, and 1–10 grace blocks")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = (args.out or ROOT / "results" / f"{args.study}-{stamp}").resolve()
    run_dir.mkdir(parents=True)
    artifact = compile_contract(args.solc)
    fixture_doc = json.loads(FIXTURES.read_text())
    modes = [Mode("preflight", False), Mode("atomic", True), Mode("atomic-grace", True, args.grace_blocks)]
    metadata = {
        "study": args.study, "seed": args.seed, "replicates": args.replicates, "burst_width": args.burst_width,
        "block_seconds": BLOCK_SECONDS, "java_options": JAVA_LIMITS, "base_close_ms": BASE_CLOSE_MS,
        "offsets_ms": OFFSETS_MS, "modes": [asdict(mode) for mode in modes],
        "contract_sha256": hashlib.sha256(CONTRACT.read_bytes()).hexdigest(),
        "fixture_sha256": hashlib.sha256(FIXTURES.read_bytes()).hexdigest(),
        "besu": subprocess.check_output([args.besu, "--version"], text=True).strip(),
        "solc": subprocess.check_output([args.solc, "--version"], text=True).splitlines()[-1],
    }
    (run_dir / "configuration.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Starting {args.study} in {run_dir}", flush=True)
    rows = []
    with launch_network(args.besu, run_dir) as network, (run_dir / "traces.jsonl").open("w") as trace:
        replay = Replay(network, artifact)
        if args.study == "pilot":
            for mode in modes:
                replay.deploy(mode)
                group = replay.pilot(mode, fixture_doc["fixtures"])
                trace.writelines(json.dumps(row) + "\n" for row in group)
                trace.flush()
                rows.extend(group)
                print(f"{mode.name}: deterministic cases passed", flush=True)
        else:
            for replicate in range(args.replicates):
                # Rotate mode order across repetitions to reduce order effects.
                for mode in modes[replicate % len(modes):] + modes[:replicate % len(modes)]:
                    replay.deploy(mode)
                    for workload in ("quiet", "burst"):
                        for trials in trial_groups(fixture_doc["fixtures"], workload, replicate,
                                                  args.seed, args.burst_width):
                            group = replay.timed_group(mode, workload, replicate, trials)
                            trace.writelines(json.dumps(row) + "\n" for row in group)
                            trace.flush()
                            rows.extend(group)
                        print(f"{mode.name}/{workload}: replicate {replicate + 1} complete", flush=True)
    (run_dir / "summary.json").write_text(json.dumps(summarize(rows), indent=2) + "\n")
    print(f"Complete: {len(rows)} case traces", flush=True)


if __name__ == "__main__":
    main()
