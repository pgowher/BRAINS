# BRAIN 2027 erasure closeout experiment

## Main result

The completed campaign contains **1,728 case executions** and 4,480 case transaction receipts. The preflight reference recorded 172 closures after a challenge already preceded the closure transaction in ledger order. The three atomic modes blocked all 586 attempts with an earlier-ordered challenge. The window experiment separately tests whether a challenge released before the nominal close can nevertheless be ordered after closure.

## Experimental design

Four Besu QBFT validators ran on one Linux host with a one-second configured block period. An assessor and a challenger signed independently and submitted through different validators. Two observers checked committed block hashes and case state. Besu 26.10.2 ran on Java 25, and the Solidity 0.8.35 contract targeted Berlin. The contract retained an opaque case ID, an evidence digest, and case status. Receipt and challenge contents stayed off-chain. Each replay used one case version and at most one challenge.

The six synthetic GridWeave conflict fixture pairs matched saved VERCLO outcomes of `CLOSED` and `DISPUTED`. The control stream supplied the digest of evidence already assessed as sufficient. The paired adverse stream supplied the challenge digest. The measured path began at observed case readiness, after that evidence assessment.

Eight seeds (20261110 through 20261117) supplied matched schedules to all four modes. Each seed included six quiet cases run one at a time and one synchronized burst of 48 cases. Four seeds ran on each of two fresh chains. Mode order was counterbalanced. Transactions were signed before timing and sent as batches by independent workers. The nominal zero-window close was scheduled 680–920 ms after the preflight read. Challenge offsets were varied around that cutoff. A seeded 5 ms stagger resolved equal planned offsets. The preflight reference checked readiness off-chain before recording closure. Atomic modes checked for an open challenge inside the close transaction. A window of `w` made the first eligible block the readiness block plus `w`.

## Results by mode and workload

| Workload | Closure mode | Closed after ordered challenge | Closed ahead of advance challenge | Closed with pending challenge | Uncontested median seconds (n) |
|---|---|---:|---:|---:|---:|
| Quiet | Preflight | 8/8 | 16/24 | 16/32 | 1.01 (16) |
| Quiet | Atomic, 0 blocks | 0/6 | 19/24 | 19/32 | 1.01 (16) |
| Quiet | Atomic, 1 block | 0/5 | 20/24 | 20/32 | 1.00 (16) |
| Quiet | Atomic, 3 blocks | 0/32 | 0/24 | 0/32 | 2.99 (16) |
| Burst | Preflight | 164/164 | 12/96 | 12/224 | 2.06 (160) |
| Burst | Atomic, 0 blocks | 0/152 | 11/96 | 11/224 | 2.06 (160) |
| Burst | Atomic, 1 block | 0/167 | 11/96 | 11/224 | 2.08 (160) |
| Burst | Atomic, 3 blocks | 0/224 | 0/96 | 0/224 | 2.97 (160) |

The three count denominators are closure attempts with an earlier-ordered challenge, cases with an advance challenge, and all challenged cases, respectively. An **advance challenge** was scheduled and released before the common nominal cutoff. That denominator remains fixed when a window delays actual close submission. A **pending challenge** was acknowledged before actual close submission and followed closure in ledger order. Ledger order uses block height followed by transaction index. For each batch, acknowledgement time is the time the full RPC response returned.

- **Quiet.** With the three-block window, 19 advance-challenge cases that closed in the immediate atomic mode were blocked, and 0 changed in the opposite direction. The median paired latency change for uncontested cases was +1.99 s across 16 matched controls. The eight seed-level median changes ranged from +1.00 to +2.02 s.
- **Burst.** With the three-block window, 11 advance-challenge cases that closed in the immediate atomic mode were blocked, and 0 changed in the opposite direction. The median paired latency change for uncontested cases was +0.82 s across 160 matched controls. The eight seed-level median changes ranged from +0.20 to +1.36 s.

The one-block eligibility floor is already satisfied when a close is submitted after the observer has seen the readiness transaction. Differences between the zero- and one-block samples in this design reflect the realized transaction schedules and receipt timing. A challenge recorded after a valid close changes the current status to `DISPUTED` and leaves the earlier closure event available for historical inspection.

## Validation and figure

All 480 planned advance challenges were released before the nominal cutoff, and 480 were acknowledged before it. The 95th-percentile challenge release lag was 1.3 ms, and the maximum was 32.3 ms. All decisions matched the expected transitions reconstructed from the plan and transaction order. The four validators used at most 1.81 GiB of combined resident memory. Minimum host available RAM was 5.52 GiB, with 672 KiB of swap growth.

**Figure 1 caption.** Panel A shows the fraction of advance-challenge cases in which an atomic closure was ordered ahead of the challenge. Panel B shows median and interquartile range for the time from observing readiness to observing the closure receipt in uncontested cases. The quiet and burst series use 16 and 160 uncontested cases per mode, respectively.

## Claim boundary and artifacts

The guard establishes a property of the recorded case history. Once an authorized challenge precedes closure in that history, the guarded transition rejects closure. The window experiment measures how the selected eligibility delays affected transaction order and observer-visible latency on this host under synthetic schedules. Ledger inclusion, challenge merits, physical erasure, and performance across geographically separated validators require separate evidence.

The data archive includes the raw traces, configuration, per-mode and per-seed tables, offset breakdowns, paired contrasts, acknowledgement sensitivity table, validation report, and the exact source needed to reproduce the analysis. The canonical run is `campaign-20261110T035318937901Z` in the BRAINS project.
