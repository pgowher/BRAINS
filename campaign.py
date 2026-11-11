"""Run paired, seeded closeout schedules on four local Besu QBFT validators."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
from itertools import groupby
import json
import os
from pathlib import Path
import platform
from queue import Empty, SimpleQueue
import random
import subprocess
import time

from experiment import CONTRACT, FIXTURES, ROOT, Mode, Replay, compile_contract, make_trials, ms_since
from qbft import BLOCK_SECONDS, JAVA_LIMITS, launch_network


MODES = (Mode("preflight", False), Mode("atomic-0", True), Mode("atomic-1", True, 1), Mode("atomic-3", True, 3))
MODE_ORDER = ((0, 1, 3, 2), (1, 2, 0, 3), (2, 3, 1, 0), (3, 0, 2, 1))
QUIET_OFFSETS_MS = (-500, -250, -80, 300, None, None)
BURST_OFFSETS_MS = (-500, -250, -80, 0, 80, 300, 550, None, None, None, None, None) * 4
NOMINAL_CLOSES_MS = tuple(range(680, 921, 20))
POLL_SECONDS = 0.020
NS_PER_MS = 1_000_000
TIE_STAGGER_MS = 5


def seeded_groups(fixtures, workload, seed):
    rng = random.Random(f"{seed}/{workload}")
    offsets = list(QUIET_OFFSETS_MS if workload == "quiet" else BURST_OFFSETS_MS)
    rng.shuffle(offsets)
    trials = make_trials(fixtures, workload, seed, offsets)
    groups = [[trial] for trial in trials] if workload == "quiet" else [trials]
    for index, group in enumerate(groups):
        yield index, group, rng.choice(NOMINAL_CLOSES_MS), bool(rng.getrandbits(1))


def observe(peer, transactions):
    receipts = peer.batch([("eth_getTransactionReceipt", tx.tx_hash) for tx in transactions])
    observed_ns = time.monotonic_ns()
    pending = []
    for tx, receipt in zip(transactions, receipts, strict=True):
        if receipt is None:
            pending.append(tx)
        else:
            if int(receipt["status"], 16) != 1:
                raise RuntimeError(f"transaction reverted: {tx.tx_hash}")
            tx.receipt = receipt
            tx.seen_ns = observed_ns
    return pending


def wait_for_receipts(peer, transactions):
    pending = transactions
    deadline = time.monotonic() + 20
    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(f"{len(pending)} ready transactions did not finalize")
        pending = observe(peer, pending)
        if pending:
            time.sleep(POLL_SECONDS)


def head(peer):
    return int(peer.call("eth_blockNumber"), 16)


def fresh_head(peer):
    previous = head(peer)
    deadline = time.monotonic() + 10
    while head(peer) <= previous:
        if time.monotonic() > deadline:
            raise TimeoutError("no new block before burst registration")
        time.sleep(POLL_SECONDS)


def sleep_until(target_ns):
    remaining = target_ns - time.monotonic_ns()
    if remaining > 0:
        time.sleep(remaining / 1_000_000_000)


class CampaignReplay(Replay):
    def prepared(self, actor, method, values, *, initial_nonce):
        return [
            actor.prepare(self.calldata(method, *words), self.address, nonce=initial_nonce + index)
            for index, words in enumerate(values)
        ]

    def ready_group(self, trials, raw_ready):
        if len(trials) > 1:
            fresh_head(self.observer)
        for trial, receipt in zip(trials, self.assessor.submit_group(raw_ready), strict=True):
            trial.ready = receipt
        wait_for_receipts(self.observer, [trial.ready for trial in trials])
        ready_blocks = {trial.ready.order[0] for trial in trials}
        if len(ready_blocks) != 1:
            raise AssertionError("one release group spans multiple ready blocks")
        (ready_block,) = ready_blocks
        states = self.observer.batch([
            ("eth_call", {"to": self.address, "data": self.calldata("statusOf(bytes32)", trial.key)},
             hex(ready_block))
            for trial in trials
        ])
        if any(int(state, 16) != 1 for state in states):
            raise AssertionError("preflight read an unexpected status")
        return ready_block

    def release_challenges(self, challenged, raw, nominal, challenge_first, released):
        for offset, items in groupby(zip(challenged, raw, strict=True), key=lambda pair: pair[0].offset_ms):
            group = list(items)
            stagger = TIE_STAGGER_MS if offset == 0 and not challenge_first else 0
            sleep_until(nominal + (offset + stagger) * NS_PER_MS)
            transactions = self.challenger.submit_group([signed for _, signed in group])
            for (trial, _), tx in zip(group, transactions, strict=True):
                trial.challenge = tx
            released.put(transactions)

    def release_closure(self, trials, raw, target, ready_block, mode, deadline, released):
        sleep_until(target)
        if mode.grace_blocks > 1:
            while head(self.observer) < ready_block + mode.grace_blocks - 1:
                if time.monotonic_ns() > deadline:
                    raise TimeoutError("closure did not become eligible")
                time.sleep(POLL_SECONDS)
            target = time.monotonic_ns()
        transactions = self.assessor.submit_group(raw)
        for trial, tx in zip(trials, transactions, strict=True):
            trial.close = tx
        released.put(transactions)
        return target

    def collect(self, workers, released, deadline):
        pending = []
        while True:
            for worker in workers:
                if worker.done():
                    worker.result()
            while True:
                try:
                    pending.extend(released.get_nowait())
                except Empty:
                    break
            pending = observe(self.observer, pending)
            if all(worker.done() for worker in workers) and released.empty() and not pending:
                return
            if time.monotonic_ns() > deadline:
                raise TimeoutError("ready, challenge and close transactions did not finish on time")
            time.sleep(POLL_SECONDS)

    def run_group(self, mode, workload, seed, wave, trials, nominal_ms, challenge_first):
        challenged = sorted(
            (trial for trial in trials if trial.offset_ms is not None), key=lambda trial: trial.offset_ms
        )
        assessor_nonce = self.assessor.nonce
        raw_ready = self.prepared(self.assessor, "registerReady(bytes32,bytes32)",
                                  [(trial.key, trial.evidence) for trial in trials], initial_nonce=assessor_nonce)
        raw_close = self.prepared(self.assessor, "recordClose(bytes32,bytes32)",
                                  [(trial.key, trial.evidence) for trial in trials],
                                  initial_nonce=assessor_nonce + len(trials))
        raw_challenge = self.prepared(self.challenger, "fileChallenge(bytes32,bytes32)",
                                      [(trial.key, trial.adverse) for trial in challenged],
                                      initial_nonce=self.challenger.nonce)
        ready_block = self.ready_group(trials, raw_ready)
        start = time.monotonic_ns()
        nominal = start + nominal_ms * NS_PER_MS
        deadline = start + 20_000 * NS_PER_MS
        close_stagger = TIE_STAGGER_MS if challenge_first and any(t.offset_ms == 0 for t in trials) else 0
        released = SimpleQueue()
        with ThreadPoolExecutor(max_workers=2) as pool:
            challenger = pool.submit(self.release_challenges, challenged, raw_challenge, nominal,
                                     challenge_first, released)
            assessor = pool.submit(self.release_closure, trials, raw_close, nominal + close_stagger * NS_PER_MS,
                                   ready_block, mode, deadline, released)
            self.collect((challenger, assessor), released, deadline)
        close_release = assessor.result()
        rows = []
        for trial in trials:
            row = self.row(trial, mode, workload, seed)
            if row["decision"] == "TOO_EARLY":
                raise AssertionError("the driver released a close before its eligibility block")
            advance = trial.offset_ms is not None and trial.offset_ms < 0
            if advance and trial.challenge.submitted_ns >= nominal:
                raise AssertionError("the dispatcher released a planned advance challenge after the common cutoff")
            row.update({
                "seed": seed,
                "wave": wave,
                "same_offset_challenge_first": challenge_first,
                "nominal_close_ms": nominal_ms,
                "nominal_from_ready_ms": ms_since(nominal, trial.ready.seen_ns),
                "close_gate_from_ready_ms": ms_since(close_release, trial.ready.seen_ns),
                "close_dispatch_lag_ms": ms_since(trial.close.submitted_ns, close_release),
                "challenge_dispatch_lag_ms": (
                    ms_since(trial.challenge.submitted_ns, nominal + (
                        trial.offset_ms + (TIE_STAGGER_MS if trial.offset_ms == 0 and not challenge_first else 0)
                    ) * NS_PER_MS)
                    if trial.challenge else None
                ),
                "advance_challenge": advance,
                "advance_ack_before_nominal": advance and trial.challenge.acknowledged_ns < nominal,
            })
            rows.append(row)
        return rows


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--besu", default=os.getenv("BESU", str(ROOT / ".tools/besu-26.10.2/bin/besu")))
    parser.add_argument("--solc", default=os.getenv("SOLC", str(ROOT / ".tools/solc")))
    parser.add_argument("--out", type=Path)
    parser.add_argument("--seed", type=int, default=20261110)
    parser.add_argument("--replicates", type=int, default=8)
    parser.add_argument("--seeds-per-chain", type=int, default=4)
    args = parser.parse_args()
    if min(args.replicates, args.seeds_per_chain) < 1:
        parser.error("replicates and seeds per chain must be positive")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = (args.out or ROOT / "results" / f"campaign-{stamp}").resolve()
    run_dir.mkdir(parents=True)
    seeds = list(range(args.seed, args.seed + args.replicates))
    fixtures = json.loads(FIXTURES.read_text())["fixtures"]
    artifact = compile_contract(args.solc)
    plan = {
        "schema": "brain-closeout-campaign/3",
        "seeds": seeds,
        "seeds_per_chain": args.seeds_per_chain,
        "modes": [asdict(mode) for mode in MODES],
        "mode_order": MODE_ORDER,
        "block_period_s": BLOCK_SECONDS,
        "nominal_closes_ms": NOMINAL_CLOSES_MS,
        "poll_s": POLL_SECONDS,
        "zero_offset_stagger_ms": TIE_STAGGER_MS,
        "submission": "pre-signed RPC batches, independent sender per actor, batch reply acknowledgement",
        "quiet_offsets_ms": QUIET_OFFSETS_MS,
        "burst_offsets_ms": BURST_OFFSETS_MS,
        "java_options": JAVA_LIMITS,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpu_count": os.cpu_count(),
        "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                          if line.startswith("model name")),
        "besu": subprocess.check_output([args.besu, "--version"], text=True).strip(),
        "solc": subprocess.check_output([args.solc, "--version"], text=True).splitlines()[-1],
        "source_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in
                          (CONTRACT, FIXTURES, ROOT / "experiment.py", ROOT / "qbft.py", Path(__file__).resolve())},
    }
    (run_dir / "configuration.json").write_text(json.dumps(plan, indent=2) + "\n")
    print(f"Starting {len(seeds)} matched schedules in {run_dir}", flush=True)
    count = 0
    with (run_dir / "traces.jsonl").open("x") as output:
        for first in range(0, len(seeds), args.seeds_per_chain):
            segment = seeds[first:first + args.seeds_per_chain]
            chain_index = first // args.seeds_per_chain + 1
            chain_dir = run_dir / f"chain-{chain_index}"
            chain_dir.mkdir()
            with launch_network(args.besu, chain_dir) as network:
                replay = CampaignReplay(network, artifact)
                for index, seed in enumerate(segment, first):
                    order = MODE_ORDER[index % len(MODE_ORDER)]
                    workloads = ("quiet", "burst") if index % 2 == 0 else ("burst", "quiet")
                    for mode_index in order:
                        mode = MODES[mode_index]
                        replay.deploy(mode)
                        for workload in workloads:
                            for wave, trials, nominal, challenge_first in seeded_groups(fixtures, workload, seed):
                                rows = replay.run_group(mode, workload, seed, wave, trials, nominal, challenge_first)
                                for row in rows:
                                    row["chain"] = chain_index
                                    output.write(json.dumps(row) + "\n")
                                output.flush()
                                count += len(rows)
                        print(f"Seed {seed} / {mode.name}: {count} case traces saved", flush=True)
    expected = len(seeds) * len(MODES) * (len(QUIET_OFFSETS_MS) + len(BURST_OFFSETS_MS))
    if count != expected:
        raise AssertionError(f"expected {expected} rows, recorded {count}")
    (run_dir / "complete.json").write_text(json.dumps({"case_traces": count}, indent=2) + "\n")
    print(f"Complete: {count} case traces", flush=True)


if __name__ == "__main__":
    main()
