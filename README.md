# Smart-meter erasure closeout experiment

This prototype tests how a recorded erasure case decision interacts with an independently submitted challenge. Four Besu QBFT validators run on one Linux host. The assessor and challenger sign with separate temporary accounts and submit through different validators. Two other validators are used to observe and cross-check committed state.

## Reproduce the paper results

The completed run is `results/campaign-20261110T035318937901Z`. Its `paper-results/` directory holds the results brief, tables, and figure.

The local tools are installed under `.tools/`. On a fresh copy, run `./setup.sh` first. Setup targets Linux x86-64 with Java 25 and Python 3.14. It checks the Besu and Solidity downloads against pinned SHA-256 digests and installs the pinned Python dependencies.

```bash
cd /home/gowher/Desktop/BRAINS
.tools/venv/bin/python campaign.py --replicates 8 --seeds-per-chain 4
python3 analyze_campaign.py results/<campaign-directory> --out results/<campaign-directory>/paper-results --plot
```

Replace `<campaign-directory>` with the directory printed by the runner. Plotting requires Matplotlib. Omit `--plot` to generate the validation report and CSV tables using the Python standard library.

`Challenge_Timing.csv` gives the acknowledgement lead and ledger position for each challenged case. With `--plot`, the analyzer also exports `Challenge_Timing.pdf`, the manuscript's quiet and burst comparison of the zero- and three-block atomic policies.

Each of eight schedule seeds is replayed under preflight and three atomic policies. Quiet runs have six single-case waves, and burst runs have one 48-case wave. Each burst registers its cases in one block. Each close window defines the first eligible block as the ready height plus 0, 1, or 3. The driver uses two fresh local networks with four matched seeds on each.

The campaign signs before timing and submits batches through independent assessor and challenger workers. It stores actual RPC release, response and receipt-observation times alongside each planned offset. An advance challenge is a challenge scheduled and released before the nominal zero-window close time. The analysis separately checks acknowledgement before that cutoff and provides a sensitivity table restricted to cases acknowledged by the cutoff in every matched mode. A batch's acknowledgement is the time its full RPC response returns.

The analyzer checks source hashes, seed matching, transaction order, eligibility and case status before exporting results. Use only a completed `brain-closeout-campaign/3` run with a `complete.json` marker. Earlier campaign directories are retained as timing calibration records.

## Calibration and conformance

```bash
cd /home/gowher/Desktop/BRAINS
.tools/venv/bin/python experiment.py pilot
.tools/venv/bin/python experiment.py sweep --replicates 1 --burst-width 24 --grace-blocks 3
```

The pilot checks an uncontested closure, a challenge committed before closure, and a challenge committed after closure, in each of the three modes. It also checks that unauthorized challenges and mismatched evidence digests are rejected. With a three-block window it checks that early closure is rejected.

The sweep pairs a schedule across modes. Quiet runs submit one case at a time. Burst runs schedule a group of independent cases together. Each group starts after the observer sees ready cases. The driver targets a nominal closure submission 750 ms later and varies challenge submissions around it, including uncontested controls. The window is enforced by the contract using the ready case's block height. Repeat runs with `--seed` and `--replicates`, and vary the window with `--grace-blocks`.

## Reading the results

- **Preflight** records an assessor's decision after an earlier status read. It provides an experimental control for the interval between the read and recording.
- **Atomic** checks for a committed challenge in the closure transaction. A later committed challenge changes current case status to `DISPUTED` and leaves the closure event in history.
- **Atomic with a challenge window** adds a delay before a case is eligible for closure. The delay is measured in blocks.

The driver orders committed transactions by block height and transaction index. A **closure after a finalized challenge** is a recorded closure for which that order already contains the challenge. A **closure while an earlier challenge was pending** is a recorded closure for which the challenge submission node acknowledged the transaction before the assessor submitted closure, yet the ledger ordered the challenge afterward. Uncontested latency is the elapsed time between the observer seeing case readiness and seeing the closure receipt. RPC timestamps come from one local monotonic clock.

Each run saves `configuration.json`, `traces.jsonl`, `summary.json`, `resources.json`, and validator logs under `results/`. The summary includes case counts and sample sizes for the latency statistics. A one-replicate sweep is a calibration run. Its observations describe the configured host and schedule.

## Evidence scope

The six fixture pairs are scale-one GridWeave conflict cases with matching saved VERCLO outcomes of `CLOSED` and `DISPUTED`. The exporter checks the receipt-stream digests against both frozen predictions. The replay treats the closed fixture as an already assessed case and turns its paired adverse stream digest into an authorized challenge marker. It keeps meter data, declarations, and challenge contents off-chain.

```bash
python3 export_fixtures.py /home/gowher/Desktop/VERCLO_PhD fixtures/gridweave.json
```

The contract handles one challenge and one recorded closure per case. It measures the ordering and publication of assertions within that case. Physical erasure, the merits of the challenge, and transaction inclusion under network failure are outside this pilot's scope.
