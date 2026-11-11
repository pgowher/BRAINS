"""Extract compact experiment inputs from saved GridWeave and VERCLO artifacts."""
import argparse
import hashlib
import json
from pathlib import Path


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def load(path):
    return json.loads(path.read_bytes())


def export(workspace):
    dataset = workspace / "GridWeave/datasets/reference"
    predictions = workspace / "VERCLO/results/reference-final-2026-10-26/predictions"
    rows = []
    for pair in load(dataset / "sealed/pairs.json"):
        if pair["operation"] != "conflict" or pair["scale"] != 1:
            continue
        labels = {
            load(dataset / "sealed/cases" / f"{unit}.json")["truth"]["outcome"]: unit
            for unit in pair["unit_ids"]
        }
        if set(labels) != {"CLOSED", "DISPUTED"}:
            continue
        control = labels["CLOSED"]
        disputed = labels["DISPUTED"]
        case = dataset / "public/cases" / control
        stream = (case / "receipt-stream.json").read_bytes()
        adverse = (dataset / "public/cases" / disputed / "receipt-stream.json").read_bytes()
        for label, unit, raw in [("CLOSED", control, stream), ("DISPUTED", disputed, adverse)]:
            report = load(predictions / f"{unit}.json")
            if report["closure_class"] != label:
                raise ValueError(f"expected {label} in the saved prediction for {unit}")
            if report["input_commitments"]["receipt_stream_sha256"] != digest(raw):
                raise ValueError(f"receipt stream differs from the saved prediction for {unit}")
        rows.append({
            "pair_id": pair["pair_id"],
            "control_unit": control,
            "disputed_unit": disputed,
            "receipt_count": len(json.loads(stream)["receipts"]),
            "evidence_sha256": digest(stream),
            "adverse_stream_sha256": digest(adverse),
        })
    if not rows:
        raise ValueError("no scale-1 CLOSED/DISPUTED conflict pairs found")
    return {
        "dataset_manifest_sha256": digest((dataset / "dataset-manifest.json").read_bytes()),
        "predictions_manifest_sha256": digest((predictions / "results-manifest.json").read_bytes()),
        "fixtures": rows,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspace", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(export(args.workspace), indent=2) + "\n")
