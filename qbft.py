"""Run four disposable Besu validators on localhost and collect resource readings."""
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import socket
import subprocess
from tempfile import TemporaryDirectory
from threading import Event, Thread
import time

from eth_account import Account
from eth_keys import keys


CHAIN_ID = 485127
BLOCK_SECONDS = 1
JAVA_LIMITS = "-Xms128m -Xmx768m -XX:ActiveProcessorCount=2"


class RpcError(RuntimeError):
    pass


@dataclass
class Rpc:
    port: int

    def _post(self, message):
        connection = HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            body = json.dumps(message)
            connection.request("POST", "/", body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            raw = response.read()
        finally:
            connection.close()
        if response.status != 200:
            raise RpcError(f"HTTP {response.status}")
        return json.loads(raw)

    @staticmethod
    def _result(result):
        if "error" in result:
            raise RpcError(result["error"])
        return result["result"]

    def call(self, method, *params):
        return self._result(self._post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))

    def batch(self, calls):
        if not calls:
            return []
        request = [{"jsonrpc": "2.0", "id": index, "method": method, "params": params}
                   for index, (method, *params) in enumerate(calls)]
        results = {row["id"]: self._result(row) for row in self._post(request)}
        return [results[index] for index in range(len(request))]


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def meminfo():
    fields = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemAvailable", "SwapFree"}:
            fields[key] = int(value.split()[0])
    return fields


@dataclass
class ResourceMonitor:
    processes: list
    before: dict
    samples: list = field(default_factory=list)

    def watch(self, done):
        started = time.monotonic()
        while not done.is_set():
            rss = []
            for process in self.processes:
                try:
                    lines = Path(f"/proc/{process.pid}/status").read_text().splitlines()
                except FileNotFoundError:
                    continue
                rss.extend(int(line.split()[1]) for line in lines if line.startswith("VmRSS:"))
            self.samples.append({
                "elapsed_s": round(time.monotonic() - started, 3),
                "validators_rss_kib": rss,
                **meminfo(),
            })
            done.wait(0.5)

    def save(self, run_dir):
        full_samples = [sample for sample in self.samples if len(sample["validators_rss_kib"]) == 4]
        if full_samples:
            peak_rss = max(sum(sample["validators_rss_kib"]) for sample in full_samples)
        else:
            peak_rss = None
        report = {
            "before": self.before,
            "max_validator_sum_rss_kib": peak_rss,
            "min_mem_available_kib": min(s["MemAvailable"] for s in self.samples),
            "min_swap_free_kib": min(s["SwapFree"] for s in self.samples),
            "samples": self.samples,
        }
        (run_dir / "resources.json").write_text(json.dumps(report, indent=2) + "\n")


@dataclass
class Network:
    peers: list
    assessor: object
    challenger: object

    def await_consensus(self, processes):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if any(proc.poll() is not None for proc in processes):
                raise RuntimeError("a Besu validator exited during startup; see validator logs")
            try:
                heights = [int(peer.call("eth_blockNumber"), 16) for peer in self.peers]
                height = min(heights)
                if height >= 2:
                    hashes = {peer.call("eth_getBlockByNumber", hex(height), False)["hash"] for peer in self.peers}
                    connections = [int(peer.call("net_peerCount"), 16) for peer in self.peers]
                    if len(hashes) == 1 and min(connections) == 3:
                        return
            except (OSError, RpcError):
                pass
            time.sleep(0.2)
        raise TimeoutError("four Besu validators did not converge; see validator logs")


@contextmanager
def launch_network(besu, run_dir):
    assessor, challenger = Account.create(), Account.create()
    genesis = {
        "config": {
            "chainId": CHAIN_ID,
            "berlinBlock": 0,
            "qbft": {"blockperiodseconds": BLOCK_SECONDS, "epochlength": 30000, "requesttimeoutseconds": 4},
        },
        "nonce": "0x0",
        "timestamp": "0x0",
        "gasLimit": "0x989680",
        "difficulty": "0x1",
        "mixHash": "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365",
        "coinbase": "0x0000000000000000000000000000000000000000",
        "alloc": {account.address[2:].lower(): {"balance": "0x21e19e0c9bab2400000"}
                  for account in (assessor, challenger)},
    }
    env = {**os.environ, "JAVA_OPTS": JAVA_LIMITS}
    with TemporaryDirectory(prefix=".chain-", dir=run_dir) as temp:
        root = Path(temp)
        config = root / "config.json"
        config.write_text(json.dumps({"genesis": genesis, "blockchain": {"nodes": {"generate": True, "count": 4}}}))
        generated = root / "generated"
        with (run_dir / "genesis.log").open("w") as log:
            subprocess.run([besu, "operator", "generate-blockchain-config", f"--config-file={config}",
                            f"--to={generated}", "--private-key-file-name=key"],
                           stdout=log, stderr=subprocess.STDOUT, env=env, check=True, timeout=60)
        p2p_ports = free_ports(8)
        rpc_ports = p2p_ports[4:]
        key_files = sorted(generated.glob("keys/*/key"))
        if len(key_files) != 4:
            raise RuntimeError("expected four generated validator keys")
        enodes = []
        for index, key_file in enumerate(key_files):
            key = keys.PrivateKey(bytes.fromhex(key_file.read_text().strip().removeprefix("0x")))
            enodes.append(f"enode://{key.public_key.to_hex()[2:]}@127.0.0.1:{p2p_ports[index]}")

        processes = []
        done = Event()
        monitor = ResourceMonitor(processes, meminfo())
        sampler = Thread(target=monitor.watch, args=(done,))
        try:
            for index, key_file in enumerate(key_files):
                node_dir = root / f"validator-{index + 1}"
                node_dir.mkdir()
                (node_dir / "static-nodes.json").write_text(json.dumps(
                    [enode for peer, enode in enumerate(enodes) if peer != index]
                ))
                command = [
                    besu, f"--genesis-file={generated / 'genesis.json'}", f"--data-path={node_dir}",
                    f"--node-private-key-file={key_file}", "--nat-method=NONE", "--discovery-enabled=false",
                    "--p2p-host=127.0.0.1", "--p2p-interface=127.0.0.1", f"--p2p-port={p2p_ports[index]}",
                    "--rpc-http-enabled", "--rpc-http-host=127.0.0.1", f"--rpc-http-port={rpc_ports[index]}",
                    "--rpc-http-api=ETH,NET,WEB3", "--min-gas-price=0",
                ]
                with (run_dir / f"validator-{index + 1}.log").open("w") as log:
                    processes.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env))
            sampler.start()
            network = Network([Rpc(port) for port in rpc_ports], assessor, challenger)
            network.await_consensus(processes)
            yield network
        finally:
            done.set()
            if sampler.ident is not None:
                sampler.join()
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if monitor.samples:
                monitor.save(run_dir)
