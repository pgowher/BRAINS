"""Validate a completed closeout campaign and export paper tables and figures."""
import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from math import floor
from pathlib import Path
import random
from statistics import median


MODE_NAMES = {"preflight": "Preflight", "atomic-0": "Atomic 0", "atomic-1": "Atomic 1", "atomic-3": "Atomic 3"}
WORKLOADS = ("quiet", "burst")


def load(path):
    return json.loads(path.read_text())


def order(tx):
    return tx["block"], tx["index"]


def quantile(values, fraction):
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (index - lower) * (ordered[upper] - ordered[lower])


def validate(run):
    config = load(run / "configuration.json")
    assert config["schema"] == "brain-closeout-campaign/3"
    source = Path(__file__).resolve().parent
    for relative, expected in config["source_sha256"].items():
        assert hashlib.sha256((source / relative).read_bytes()).hexdigest() == expected, relative
    fixtures = load(source / "fixtures/gridweave.json")["fixtures"]
    rows = [json.loads(line) for line in (run / "traces.jsonl").read_text().splitlines()]
    assert len(rows) == load(run / "complete.json")["case_traces"]
    mode_config = {mode["name"]: mode for mode in config["modes"]}
    assert tuple(mode_config) == tuple(MODE_NAMES)
    seeds = config["seeds"]
    per_seed = defaultdict(list)
    for row in rows:
        per_seed[row["seed"], row["mode"], row["workload"]].append(row)
    assert set(per_seed) == {(seed, mode, load_name) for seed in seeds for mode in mode_config for load_name in WORKLOADS}
    receipt_ids = set()
    contracts = set()
    for seed_index, seed in enumerate(seeds):
        chain_index = seed_index // config["seeds_per_chain"] + 1
        for mode, params in mode_config.items():
            address, = {r["contract"] for load_name in WORKLOADS for r in per_seed[seed, mode, load_name]}
            assert (chain_index, address) not in contracts
            contracts.add((chain_index, address))
            for workload in WORKLOADS:
                rng = random.Random(f"{seed}/{workload}")
                offsets = list(config[f"{workload}_offsets_ms"])
                rng.shuffle(offsets)
                waves = len(offsets) if workload == "quiet" else 1
                schedule = [(rng.choice(config["nominal_closes_ms"]), bool(rng.getrandbits(1))) for _ in range(waves)]
                cohort = per_seed[seed, mode, workload]
                assert len(cohort) == len(offsets)
                assert len({r["ready"]["block"] for r in cohort}) == waves
                for case_index, (row, offset) in enumerate(zip(cohort, offsets, strict=True)):
                    fixture_id = fixtures[case_index % len(fixtures)]["pair_id"]
                    identity = f"{workload}/{seed}/{case_index}/{fixture_id}"
                    wave = case_index if workload == "quiet" else 0
                    nominal, tie_break = schedule[wave]
                    assert row["case_id"] == hashlib.sha256(identity.encode()).hexdigest()
                    assert row["fixture_pair"] == fixture_id and row["offset_ms"] == offset
                    assert row["chain"] == chain_index and row["wave"] == wave
                    assert row["nominal_close_ms"] == nominal and row["same_offset_challenge_first"] == tie_break
                    assert row["grace_blocks"] == params["grace_blocks"]
                    ready, close, challenge = row["ready"], row["close"], row["challenge"]
                    assert (challenge is not None) == (offset is not None)
                    assert close["block"] >= ready["block"] + params["grace_blocks"]
                    for tx in (ready, close, challenge):
                        if tx is None:
                            continue
                        receipt_id = chain_index, tx["hash"]
                        assert receipt_id not in receipt_ids
                        receipt_ids.add(receipt_id)
                        assert tx["submit_ms"] <= tx["ack_ms"] and tx["submit_ms"] <= tx["seen_ms"]
                    assert order(ready) < order(close)
                    assert not challenge or order(ready) < order(challenge)
                    assert ready["seen_ms"] == 0
                    assert close["submit_ms"] >= row["close_gate_from_ready_ms"] >= row["nominal_from_ready_ms"]
                    prior = challenge is not None and order(challenge) < order(close)
                    blocked = params["guarded"] and prior
                    assert row["decision"] == ("BLOCKED" if blocked else "RECORDED")
                    status = "DISPUTED" if blocked or (challenge is not None and not prior) else "CLOSED"
                    assert row["final_status"] == status
                    assert row["recorded_after_finalized_challenge"] == (prior and not blocked)
                    if challenge and challenge["ack_ms"] != close["submit_ms"]:
                        pending = not prior and challenge["ack_ms"] < close["submit_ms"]
                        assert row["recorded_while_earlier_challenge_pending"] == pending
                    assert row["advance_challenge"] == (offset is not None and offset < 0)
                    if row["advance_challenge"]:
                        assert challenge["submit_ms"] < row["nominal_from_ready_ms"]
                        if challenge["ack_ms"] != row["nominal_from_ready_ms"]:
                            assert row["advance_ack_before_nominal"] == (
                                challenge["ack_ms"] < row["nominal_from_ready_ms"]
                            )
                    else:
                        assert not row["advance_ack_before_nominal"]
    challenge_lag = [r["challenge_dispatch_lag_ms"] for r in rows if r["challenge"]]
    advance_lag = [r["challenge_dispatch_lag_ms"] for r in rows if r["advance_challenge"]]
    close_lag = [r["close_dispatch_lag_ms"] for r in rows]
    close_rpc = [r["close"]["ack_ms"] - r["close"]["submit_ms"] for r in rows]
    challenge_rpc = [r["challenge"]["ack_ms"] - r["challenge"]["submit_ms"] for r in rows if r["challenge"]]
    resource_reports = [load(path) for path in sorted(run.glob("chain-*/resources.json"))]
    assert len(resource_reports) == max(row["chain"] for row in rows)
    audit = {
        "result_directory": run.name,
        "case_rows": len(rows),
        "unique_transaction_receipts": len(receipt_ids),
        "mode_deployments": len(contracts),
        "chain_instances": len(resource_reports),
        "missing_or_unexpected_decisions": 0,
        "advance_releases_missing_nominal_cutoff": 0,
        "advance_acknowledgements_after_nominal_cutoff": sum(
            r["advance_challenge"] and not r["advance_ack_before_nominal"] for r in rows
        ),
        "challenge_dispatch_lag_p95_ms": quantile(challenge_lag, .95),
        "challenge_dispatch_lag_max_ms": max(challenge_lag),
        "advance_challenge_dispatch_lag_p95_ms": quantile(advance_lag, .95),
        "advance_challenge_dispatch_lag_max_ms": max(advance_lag),
        "close_batch_dispatch_lag_p95_ms": quantile(close_lag, .95),
        "close_batch_dispatch_lag_max_ms": max(close_lag),
        "close_rpc_ack_p95_ms": quantile(close_rpc, .95),
        "challenge_rpc_ack_p95_ms": quantile(challenge_rpc, .95),
        "validator_peak_sum_rss_gib": round(max(r["max_validator_sum_rss_kib"] for r in resource_reports) / 2**20, 2),
        "minimum_system_available_gib": round(min(r["min_mem_available_kib"] for r in resource_reports) / 2**20, 2),
        "maximum_swap_growth_kib": max(r["before"]["SwapFree"] - r["min_swap_free_kib"] for r in resource_reports),
        "trace_sha256": hashlib.sha256((run / "traces.jsonl").read_bytes()).hexdigest(),
    }
    return config, rows, audit


def counts(rows):
    challenge = [r for r in rows if r["challenge"]]
    advance = [r for r in challenge if r["advance_challenge"]]
    controls = [r for r in rows if not r["challenge"]]
    latencies = [r["close"]["seen_ms"] for r in controls]
    overtaken = [r for r in advance if r["decision"] == "RECORDED" and order(r["close"]) < order(r["challenge"])]
    return {
        "cases": len(rows),
        "challenged": len(challenge),
        "challenge_preceded_close": sum(order(r["challenge"]) < order(r["close"]) for r in challenge),
        "recorded_after_prior_challenge": sum(r["recorded_after_finalized_challenge"] for r in challenge),
        "recorded_while_pending_at_actual_send": sum(r["recorded_while_earlier_challenge_pending"] for r in challenge),
        "advance_challenges": len(advance),
        "advance_ack_before_nominal": sum(r["advance_ack_before_nominal"] for r in advance),
        "advance_recorded_after_prior_challenge": sum(r["recorded_after_finalized_challenge"] for r in advance),
        "advance_recorded_before_challenge": len(overtaken),
        "advance_recorded_before_challenge_same_block": sum(
            r["close"]["block"] == r["challenge"]["block"] for r in overtaken
        ),
        "closed": sum(r["decision"] == "RECORDED" for r in rows),
        "blocked": sum(r["decision"] == "BLOCKED" for r in rows),
        "uncontested": len(controls),
        "uncontested_median_ms": median(latencies) if latencies else None,
        "uncontested_p25_ms": quantile(latencies, .25) if latencies else None,
        "uncontested_p75_ms": quantile(latencies, .75) if latencies else None,
        "uncontested_p95_ms": quantile(latencies, .95) if latencies else None,
    }


def aggregate(rows, keys):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[key] for key in keys)].append(row)
    return [dict(zip(keys, group, strict=True), **counts(values)) for group, values in groups.items()]


def compare(rows):
    out = []
    for workload in WORKLOADS:
        zero = {r["case_id"]: r for r in rows if r["workload"] == workload and r["mode"] == "atomic-0"}
        for mode in ("atomic-1", "atomic-3"):
            other = {r["case_id"]: r for r in rows if r["workload"] == workload and r["mode"] == mode}
            assert zero.keys() == other.keys()
            improved = worsened = unchanged_recorded = unchanged_blocked = 0
            latency_changes = []
            seed_changes = defaultdict(list)
            for case_id, first in zero.items():
                second = other[case_id]
                if first["advance_challenge"]:
                    if first["decision"] == "RECORDED" and second["decision"] == "BLOCKED":
                        improved += 1
                    elif first["decision"] == "BLOCKED" and second["decision"] == "RECORDED":
                        worsened += 1
                    elif first["decision"] == "RECORDED":
                        unchanged_recorded += 1
                    else:
                        unchanged_blocked += 1
                if first["challenge"] is None:
                    change = second["close"]["seen_ms"] - first["close"]["seen_ms"]
                    latency_changes.append(change)
                    seed_changes[first["seed"]].append(change)
            seed_medians = [median(values) for values in seed_changes.values()]
            out.append({
                "workload": workload, "mode": mode, "reference": "atomic-0",
                "advance_pairs": improved + worsened + unchanged_recorded + unchanged_blocked,
                "recorded_to_blocked": improved, "blocked_to_recorded": worsened,
                "recorded_in_both": unchanged_recorded, "blocked_in_both": unchanged_blocked,
                "uncontested_pairs": len(latency_changes), "paired_median_latency_change_ms": median(latency_changes),
                "min_seed_median_latency_change_ms": min(seed_medians),
                "max_seed_median_latency_change_ms": max(seed_medians),
            })
    return out


def common_acknowledgements(rows):
    paired = defaultdict(list)
    for row in rows:
        if row["advance_challenge"]:
            paired[row["case_id"]].append(row)
    common = [row for pair in paired.values() if all(r["advance_ack_before_nominal"] for r in pair) for row in pair]
    return aggregate(common, ("mode", "workload"))


def challenge_timing(rows):
    timing = []
    for row in rows:
        challenge, close = row["challenge"], row["close"]
        if challenge is None:
            continue
        block_delta = challenge["block"] - close["block"]
        if block_delta:
            position = "Earlier block" if block_delta < 0 else "Later block"
        else:
            position = "Earlier in same block" if order(challenge) < order(close) else "Later in same block"
        timing.append({
            "mode": row["mode"],
            "workload": row["workload"],
            "seed": row["seed"],
            "case_id": row["case_id"],
            "ack_lead_ms": round(close["submit_ms"] - challenge["ack_ms"], 3),
            "challenge_block_delta": block_delta,
            "ledger_position": position,
            "close_decision": row["decision"],
        })
    return timing


def write_csv(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def plot(rows, summary, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator, PercentFormatter

    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 7.5, "axes.spines.top": False,
        "axes.spines.right": False, "axes.labelsize": 7.5, "axes.titlesize": 8,
        "svg.fonttype": "none", "pdf.fonttype": 42,
    })
    lookup = {(r["mode"], r["workload"]): r for r in summary}
    seed_summary = aggregate(rows, ("seed", "mode", "workload"))
    by_seed = {(r["seed"], r["mode"], r["workload"]): r for r in seed_summary}
    seeds = sorted({r["seed"] for r in rows})
    seed_labels = [f"S{i}" for i in range(1, len(seeds) + 1)] + ["Pooled"]
    atomic_modes = ("atomic-0", "atomic-1", "atomic-3")
    windows = ("0", "1", "3")
    colors = ("#155B7B", "#9B4926")
    markers = ("o", "s")
    cmap = LinearSegmentedColormap.from_list(
        "closed_ahead", ("#f2f5f7", "#c0d6e5", "#5384a4", "#173c57")
    )

    fig = plt.figure(figsize=(7.12, 2.92 + max(0, len(seeds) - 8) * .18))
    grid = fig.add_gridspec(1, 2, width_ratios=(.43, .57), left=.070, right=.985,
                           bottom=.23, top=.81, wspace=.27)
    advance_grid = grid[0].subgridspec(1, 2, wspace=.12)
    latency_grid = grid[1].subgridspec(1, 2, width_ratios=(1.25, 1), wspace=.40)
    heatmaps = [fig.add_subplot(advance_grid[i]) for i in range(2)]
    distribution = fig.add_subplot(latency_grid[0])
    contrasts = fig.add_subplot(latency_grid[1])
    fig.text(heatmaps[0].get_position().x0, .965, "(a)  Closures ahead of advance challenges",
             ha="left", va="top", weight="bold", fontsize=9)
    fig.text(distribution.get_position().x0, .965, "(b)  Uncontested closeout",
             ha="left", va="top", weight="bold", fontsize=9)

    for widx, workload in enumerate(WORKLOADS):
        axis = heatmaps[widx]
        cells = [[by_seed[seed, mode, workload] for mode in atomic_modes] for seed in seeds]
        cells.append([lookup[mode, workload] for mode in atomic_modes])
        fractions = [[cell["advance_recorded_before_challenge"] / cell["advance_challenges"]
                      for cell in line] for line in cells]
        x_edges = [x - .5 for x in range(len(windows) + 1)]
        y_edges = [y - .5 for y in range(len(cells) + 1)]
        mesh = axis.pcolormesh(x_edges, y_edges, fractions, vmin=0, vmax=1, cmap=cmap,
                               edgecolors="white", linewidth=.55)
        axis.invert_yaxis()
        for y, line in enumerate(cells):
            for x, cell in enumerate(line):
                count, total = cell["advance_recorded_before_challenge"], cell["advance_challenges"]
                axis.text(x, y, f"{count}/{total}", ha="center", va="center",
                          color="white" if fractions[y][x] >= .72 else "#172832",
                          weight="bold" if y == len(seeds) else "normal", fontsize=7.2)
        axis.set_title(workload.capitalize(), pad=4)
        axis.set_xticks(range(len(windows)), windows)
        axis.set_yticks(range(len(seed_labels)), seed_labels if widx == 0 else [])
        axis.tick_params(length=0, pad=2)
        axis.axhline(len(seeds) - .5, color="#385163", linewidth=1.1)
        for spine in axis.spines.values():
            spine.set_visible(False)

    center = (heatmaps[0].get_position().x0 + heatmaps[1].get_position().x1) / 2
    fig.text(center, .125, "Eligibility window (blocks)", ha="center", va="center")
    color_axis = fig.add_axes([center - .043, .058, .135, .020])
    colorbar = fig.colorbar(mesh, cax=color_axis, orientation="horizontal", ticks=(0, .5, 1),
                           format=PercentFormatter(1, decimals=0))
    colorbar.outline.set_visible(False)
    colorbar.ax.tick_params(length=2, pad=1, labelsize=6.5)
    fig.text(center - .055, .068, "Share closed ahead", ha="right", va="center", fontsize=7)

    controls = defaultdict(list)
    for row in rows:
        if row["challenge"] is None and row["mode"] in atomic_modes:
            controls[row["mode"], row["workload"]].append(row)
    jitter = random.Random(0)
    row_labels = []
    for midx, mode in enumerate(atomic_modes):
        for widx, workload in enumerate(WORKLOADS):
            y = 2 * midx + widx
            row_labels.append(f"{windows[midx]} {workload[0].upper()}")
            sample = [r["close"]["seen_ms"] / 1000 for r in controls[mode, workload]]
            offsets = [y - .12 + jitter.uniform(-.14, .14) for _ in sample]
            color, marker = colors[widx], markers[widx]
            distribution.scatter(sample, offsets, s=7, c=color, marker=marker, linewidths=0,
                                 alpha=.55 if workload == "quiet" else .27)
            distribution.hlines(y + .18, quantile(sample, .25), quantile(sample, .75),
                                color=color, linewidth=2.2)
            distribution.plot(median(sample), y + .18, "|", color="#172832",
                              markersize=5.5, markeredgewidth=1.2)
    distribution.set_yticks(range(len(row_labels)), row_labels)
    distribution.set_ylim(len(row_labels) - .5, -.5)
    distribution.set_title("Latency distribution", pad=4)
    distribution.set_xlabel("Observed latency (s)", labelpad=2)
    for y in (1.5, 3.5):
        distribution.axhline(y, color="#dce4e9", linewidth=.7)

    for widx, workload in enumerate(WORKLOADS):
        immediate = {r["case_id"]: r["close"]["seen_ms"] for r in controls["atomic-0", workload]}
        differences = defaultdict(list)
        for row in controls["atomic-3", workload]:
            differences[row["seed"]].append((row["close"]["seen_ms"] - immediate[row["case_id"]]) / 1000)
        pooled = median(change for sample in differences.values() for change in sample)
        y = [i + (-.15 if widx == 0 else .15) for i in range(len(seeds))]
        contrasts.scatter([median(differences[seed]) for seed in seeds], y,
                          color=colors[widx], marker=markers[widx], s=16, zorder=3)
        contrasts.scatter(pooled, len(seeds) + (-.15 if widx == 0 else .15),
                          color=colors[widx], marker="D", s=20, zorder=3)
    contrasts.set_yticks(range(len(seed_labels)), seed_labels)
    contrasts.set_ylim(len(seed_labels) - .5, -.5)
    contrasts.axhline(len(seeds) - .5, color="#a0afba", linewidth=.8)
    contrasts.axvline(0, color="#a0afba", linewidth=.75)
    contrasts.set_title("3 vs. 0 blocks", pad=4)
    contrasts.set_xlabel("Paired latency change (s)", labelpad=2)
    distribution.xaxis.set_major_locator(MaxNLocator(4, steps=[1, 2, 2.5, 5, 10]))
    contrasts.xaxis.set_major_locator(MaxNLocator(5, steps=[1, 2, 2.5, 5, 10]))

    for axis in (distribution, contrasts):
        axis.set_axisbelow(True)
        axis.grid(axis="x", color="#dce4e9", linewidth=.65)
        axis.tick_params(axis="y", length=0, pad=3)
        axis.tick_params(axis="x", length=2, pad=2)
        axis.spines[["left", "bottom"]].set_color("#a0afba")
    legend = [Line2D([], [], color=color, marker=marker, linestyle="none", markersize=4,
                     label=workload.capitalize())
              for color, marker, workload in zip(colors, markers, WORKLOADS, strict=True)]
    fig.legend(handles=legend, loc="lower center", bbox_to_anchor=(.76, .024),
               ncol=2, frameon=False, handletextpad=.35, columnspacing=1.2)

    fig.savefig(output / "Figure_1.svg")
    fig.savefig(output / "Figure_1.png", dpi=420)
    fig.savefig(output / "Figure_1.pdf", metadata={"CreationDate": None, "ModDate": None})
    plt.close(fig)


def plot_challenge_timing(timing, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    lanes = ("Earlier block", "Earlier in same block", "Later in same block", "Later block")
    bin_width_ms = 100
    policies = (
        ("atomic-0", "0 blocks", "#155B7B", "o", -.14),
        ("atomic-3", "3 blocks", "#9B4926", "s", .14),
    )
    with plt.rc_context({
        "font.family": "DejaVu Sans", "font.size": 8,
        "axes.spines.top": False, "axes.spines.right": False, "pdf.fonttype": 42,
    }):
        fig, axes = plt.subplots(1, 2, figsize=(7.12, 2.38), sharex=True, sharey=True)
        fig.subplots_adjust(left=.20, right=.987, bottom=.30, top=.87, wspace=.13)
        for axis, workload in zip(axes, WORKLOADS, strict=True):
            axis.set_title(workload.capitalize(), loc="left", weight="bold", fontsize=9, pad=6)
            axis.axvline(0, color="#525d64", linestyle="--", linewidth=.8, zorder=2)
            for lane in range(len(lanes)):
                axis.axhline(lane, color="#dce4e9", linewidth=.65, zorder=0)
            for mode, _, color, marker, offset in policies:
                # The zero boundary keeps negative and positive leads in separate bins.
                groups = Counter(
                    (floor(row["ack_lead_ms"] / bin_width_ms), row["ledger_position"])
                    for row in timing
                    if row["mode"] == mode and row["workload"] == workload
                )
                axis.scatter(
                    [(index + .5) * bin_width_ms / 1000 for index, _ in groups],
                    [lanes.index(position) + offset for _, position in groups],
                    s=[4 * count for count in groups.values()],
                    c=color, marker=marker, alpha=.72, linewidths=0, zorder=3,
                )
            axis.set_yticks(range(len(lanes)), lanes)
            axis.set_ylim(len(lanes) - .5, -.5)
            axis.tick_params(axis="y", length=0, pad=4)
            axis.tick_params(axis="x", length=2)
            axis.spines[["left", "bottom"]].set_color("#a0afba")
        fig.supxlabel("Challenge acknowledgement lead relative to close submission (s)", x=.596, y=.15)
        handles = [Line2D([], [], color=color, marker=marker, linestyle="none", markersize=4,
                          label=label)
                   for _, label, color, marker, _ in policies]
        fig.legend(handles=handles, loc="lower center", bbox_to_anchor=(.596, .012),
                   ncol=2, frameon=False)
        fig.savefig(output / "Challenge_Timing.pdf", metadata={"CreationDate": None, "ModDate": None})
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()
    config, rows, audit = validate(args.run)
    args.out.mkdir(parents=True, exist_ok=True)
    summary = aggregate(rows, ("mode", "workload"))
    seed = aggregate(rows, ("seed", "mode", "workload"))
    offsets = aggregate(rows, ("mode", "workload", "offset_ms"))
    contrasts = compare(rows)
    timing = challenge_timing(rows)
    write_csv(args.out / "Results_by_mode.csv", summary)
    write_csv(args.out / "Results_by_seed.csv", seed)
    write_csv(args.out / "Results_by_offset.csv", offsets)
    write_csv(args.out / "Matched_contrasts.csv", contrasts)
    write_csv(args.out / "Common_ack_sensitivity.csv", common_acknowledgements(rows))
    write_csv(args.out / "Challenge_Timing.csv", timing)
    (args.out / "Validation.json").write_text(json.dumps(audit, indent=2) + "\n")
    if args.plot:
        plot(rows, summary, args.out)
        plot_challenge_timing(timing, args.out)
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
