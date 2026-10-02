"""Rebuild SpotLake AWS snapshots with placement columns recomputed by the current logic.

The stored 2025 snapshots were produced by three collector generations:
  * until 2025-02-14, snapshots carry a target capacity 1 SPS queried every run and no T2/T3;
    target capacities 5..50 were collected separately into s3://sps-query-data/aws/
    (until 2025-02-14 00:10);
  * from 2025-02-13, one target capacity (1, 5, ..., 50) is queried per run and saved to
    s3://spotlake/rawdata/aws/sps/; snapshots built from it (SPS/T2/T3 carried between runs)
    start on 2025-02-15;
  * T2/T3 conditions used `== 2` / `== 3` during 2025 and SPS promotion started 2025-04-03.

This script replays the per-target-capacity placement scores through the current service logic
(`compare_max_instance`, vendored below) from a warm-up start, and rewrites every snapshot with:
  * SPS, T3, T2 recomputed for every row (all 11 columns in every file),
  * missing values written as empty cells instead of -1,
  * exact duplicate rows removed (present from 2025-10-21),
  * all other columns unchanged.

Replay order per 10-minute slot:
  * before CUTOVER: target capacity 1 from the snapshot's own SPS column, then the slot's
    rotating target capacity query: the legacy file, or else the new per-target file when its
    capacity is not 1 (2025-02-14 00:20 to 23:50 has only the new files);
  * from CUTOVER: the single per-target file of the slot.
Slots without a snapshot still apply their query to the state, using the previous snapshot's
rows, but produce no output file (99 such slots in 2025).

Usage:
    python reprocess_2025.py --out ../build                      # full 2025 build
    python reprocess_2025.py --out ../build-check --warmup-start 2026-08-25 \
        --start 2026-08-28 --end 2026-09-01 --compare-only        # fidelity check
"""
import argparse
import gzip
import io
import pickle
import sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config

SNAP_BUCKET = "spotlake"
LEGACY_BUCKET = "sps-query-data"
CUTOVER = datetime(2025, 2, 15)   # first snapshot built by the per-target collector
STEP = timedelta(minutes=10)
KEYS = ["InstanceType", "AZ"]
COLUMNS = ["Time", "InstanceType", "Region", "AZ", "SPS", "T3", "T2",
           "IF", "OndemandPrice", "SpotPrice", "Savings"]
SENTINEL_COLS = ["IF", "OndemandPrice", "SpotPrice", "Savings"]


# Copied verbatim from ddps-lab/spotlake@1424fb2
# collector/spot-dataset/aws/batch/merge/compare_data.py (current service logic).
def compare_max_instance(previous_df, new_df, target_capacity):
    fallback_dict = {50:45, 45:40, 40:35, 35:30, 30:25, 25:20, 20:15, 15:10, 10:5, 5:1, 1:0}
    fallback_val = fallback_dict.get(target_capacity, 0)

    spotlake_df = new_df.copy()

    merged_df = pd.merge(
        spotlake_df,
        previous_df[["InstanceType", "AZ", "SPS", "T3", "T2"]],
        on=["InstanceType", "AZ"],
        how="left",
        suffixes=("", "_prev")
    )

    # Fill NaN values for _prev columns (new workloads that don't exist in previous data)
    merged_df["T3_prev"] = merged_df["T3_prev"].fillna(0)
    merged_df["T2_prev"] = merged_df["T2_prev"].fillna(0)
    merged_df["SPS_prev"] = merged_df["SPS_prev"].fillna(merged_df["SPS"])

    # Fix SPS when single node SPS
    if target_capacity == 1:
        merged_df["SPS"] = merged_df["SPS"].combine_first(merged_df["SPS_prev"])

    # Merge single node SPS with multi node SPS if (multi node SPS) > (single node SPS)
    merged_df.loc[(merged_df["SPS"] > merged_df["SPS_prev"]), "SPS_prev"] = merged_df["SPS"]

    # Calculate T3
    merged_df["T3"] = np.where(
        merged_df["SPS"] >= 3,
        np.maximum(merged_df["T3"], merged_df["T3_prev"]),
        np.minimum(fallback_val, merged_df["T3_prev"])
    )

    # Calculate T2
    merged_df["T2"] = np.where(
        merged_df["SPS"] >= 2,
        np.maximum(merged_df["T2"], merged_df["T2_prev"]),
        np.minimum(fallback_val, merged_df["T2_prev"])
    )

    if target_capacity == 1:
        # When SPS lower than condition, set T3 or T2 to 0
        merged_df.loc[merged_df["SPS"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["SPS"] < 2, "T2"] = 0
    else:
        # When SPS lower than condition, set T3 or T2 to 0
        merged_df.loc[merged_df["SPS_prev"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["SPS_prev"] < 2, "T2"] = 0
        # Fix SPS to Single node SPS
        merged_df["SPS"] = merged_df["SPS_prev"]

    # Convert to int
    for col in ["SPS", "T2", "T3"]:
        merged_df[col] = merged_df[col].astype("int64")

    # Drop unnecessary columns
    merged_df.drop(columns=["T3_prev", "T2_prev", "SPS_prev"], inplace=True)

    return merged_df


def parse_slot(name, fmt):
    return datetime.strptime(name, fmt)


class Source:
    """Reads objects from S3, or from a local mirror laid out as <root>/<bucket>/<key>."""

    def __init__(self, s3, root=None):
        self.s3, self.root = s3, root

    def list(self, bucket, prefix):
        if self.root:
            base = self.root / bucket
            for p in sorted((base / prefix).rglob("*")):
                if p.is_file():
                    yield p.relative_to(base).as_posix()
            return
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                yield o["Key"]

    def get(self, bucket, key):
        if self.root:
            return (self.root / bucket / key).read_bytes()
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def months(start, end):
    m = datetime(start.year, start.month, 1)
    while m < end:
        yield m
        m = datetime(m.year + (m.month == 12), m.month % 12 + 1, 1)


def build_index(src, start, end):
    """Map each slot to its snapshot key and its per-target-capacity query (key, tc)."""
    snaps, legacy, new = {}, {}, {}
    for m in months(start, end):
        for key in src.list(SNAP_BUCKET, f"rawdata/aws/{m:%Y/%m}/"):
            if not key.endswith(".csv.gz"):
                continue                               # partial downloads and other files
            t = parse_slot(key[len("rawdata/aws/"):-len(".csv.gz")], "%Y/%m/%d/%H-%M-%S")
            snaps[t] = key
        for bucket, prefix, ext in ((LEGACY_BUCKET, "aws/", ".csv.gz"),
                                    (SNAP_BUCKET, "rawdata/aws/sps/", ".pkl.gz")):
            for key in src.list(bucket, f"{prefix}{m:%Y/%m}/"):
                if not key.endswith(ext):
                    continue
                stem = key[len(prefix):-len(ext)]      # YYYY/MM/DD/HH-MM_sps_TC
                when, _, tc = stem.rpartition("_sps_")
                t = parse_slot(when, "%Y/%m/%d/%H-%M")
                # The service takes the first listed file when a slot has several.
                (legacy if bucket == LEGACY_BUCKET else new).setdefault(t, (bucket, key, int(tc)))
    queries = {}
    for t in set(legacy) | set(new):
        if t >= CUTOVER:
            q = new.get(t)
        else:
            q = legacy.get(t) or (new[t] if t in new and new[t][2] != 1 else None)
        if q:
            queries[t] = q
    return snaps, queries


def load_slot(src, t, snap_key, query):
    snap = None
    if snap_key:
        snap = pd.read_csv(io.BytesIO(src.get(SNAP_BUCKET, snap_key)), compression="gzip")
    q = None
    if query:
        bucket, key, tc = query
        raw = src.get(bucket, key)
        if key.endswith(".pkl.gz"):
            qdf = pickle.loads(gzip.decompress(raw))
        else:
            qdf = pd.read_csv(io.BytesIO(raw), compression="gzip")
        q = (qdf[["InstanceType", "Region", "AZ", "SPS"]], tc)
    return t, snap, q


def query_frame(rows, q, tc):
    """Rows of this snapshot joined with one target-capacity result, as the service builds them."""
    q = q.drop_duplicates(KEYS)
    df = rows.merge(q, on=["InstanceType", "Region", "AZ"], how="left")
    # Current sps_query_api rule: T3/T2 record the target capacity when the score reaches 3/2.
    df["T3"] = np.where(df["SPS"] >= 3, tc, 0)
    df["T2"] = np.where(df["SPS"] >= 2, tc, 0)
    df["SPS"] = df["SPS"].fillna(-1).astype(int)
    return df


def step(prev, rows, q, tc):
    cur = query_frame(rows, q, tc)
    return cur if prev is None else compare_max_instance(prev, cur, tc)


def carry(prev, rows):
    """No query for this slot: keep the previous state for the snapshot's rows."""
    if prev is None:
        cur = rows.assign(SPS=-1, T3=0, T2=0)
    else:
        cur = rows.merge(prev[KEYS + ["SPS", "T3", "T2"]], on=KEYS, how="left")
        cur["SPS"] = cur["SPS"].fillna(-1).astype(int)
        cur[["T3", "T2"]] = cur[["T3", "T2"]].fillna(0).astype(int)
    return cur


def finalize(snap, state):
    out = snap.copy()
    for c in ["SPS", "T3", "T2"]:
        out[c] = state[c].to_numpy()
    out["SPS"] = out["SPS"].where(out["SPS"] >= 0).astype("Int64")
    for c in ["T3", "T2"]:
        out[c] = out[c].where(out["SPS"].notna()).astype("Int64")
    for c in SENTINEL_COLS:
        out[c] = out[c].where(out[c] != -1)
    out["Savings"] = out["Savings"].astype("Int64")
    return out[COLUMNS]


def compare_stats(t, snap, out):
    stat = {"Time": t, "rows": len(out)}
    old_sps = snap["SPS"].where(snap["SPS"] >= 0)
    stat["sps_diff"] = int((old_sps.fillna(-9).to_numpy() != out["SPS"].fillna(-9).to_numpy()).sum())
    for c in ["T3", "T2"]:
        if c in snap:
            old = snap[c].where(old_sps.notna())
            stat[c.lower() + "_diff"] = int((old.fillna(-9).to_numpy() != out[c].fillna(-9).to_numpy()).sum())
    stat["t2_lt_t3"] = int((out["T2"] < out["T3"]).fillna(False).sum())
    return stat


def write_csv(path, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False, compression={"method": "gzip", "compresslevel": 6, "mtime": 0})
    tmp.rename(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--warmup-start", default="2024-12-01")
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default="2026-01-01")
    ap.add_argument("--profile", default="spotrank_jaeil")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--compare-only", action="store_true", help="skip writing snapshots")
    ap.add_argument("--local-root", type=Path,
                    help="read inputs from a local mirror <root>/<bucket>/<key> instead of S3")
    args = ap.parse_args()

    warm, start, end = (datetime.fromisoformat(x) for x in (args.warmup_start, args.start, args.end))
    s3 = boto3.Session(profile_name=args.profile).client(
        "s3", region_name="us-west-2",
        config=Config(max_pool_connections=args.workers * 2, retries={"mode": "adaptive"}))

    src = Source(s3, args.local_root)
    snaps, queries = build_index(src, warm, end)
    slots = []
    t = warm
    while t < end:
        if t in snaps or t in queries:
            slots.append(t)
        t += STEP
    print(f"slots={len(slots)} with snapshot={sum(1 for s in slots if s in snaps)} "
          f"with query={sum(1 for s in slots if s in queries)}", flush=True)

    args.out.mkdir(parents=True, exist_ok=True)
    stats, carried, dup_rows, no_snapshot = [], 0, 0, 0
    prev, last_rows = None, None
    with ThreadPoolExecutor(args.workers) as loader, ThreadPoolExecutor(4) as writer:
        pending = deque()
        it = iter(slots)
        writes = deque()

        def refill():
            for s in it:
                pending.append(loader.submit(load_slot, src, s, snaps.get(s), queries.get(s)))
                if len(pending) >= args.workers * 4:
                    break

        refill()
        done = 0
        while pending:
            t, snap, q = pending.popleft().result()
            refill()
            done += 1
            if snap is None:
                # Query without a snapshot: advance the state on the previous rows, no output.
                if last_rows is not None and prev is not None:
                    prev = step(prev, last_rows, *q)
                no_snapshot += 1
                continue
            # From 2025-10-21 some snapshots repeat identical rows; keep one copy.
            dup_rows += int(snap.duplicated().sum())
            snap = snap.drop_duplicates().reset_index(drop=True)
            if snap.duplicated(KEYS).any():
                raise ValueError(f"{t}: conflicting rows for the same InstanceType and AZ")
            rows = last_rows = snap[["InstanceType", "Region", "AZ"]]

            if t < CUTOVER:
                tc1 = snap[["InstanceType", "Region", "AZ", "SPS"]]
                prev = step(prev, rows, tc1, 1)
                if q:
                    prev = step(prev, rows, *q)
            elif q:
                prev = step(prev, rows, *q)
            else:
                prev = carry(prev, rows)
                carried += 1

            if start <= t < end:
                out = finalize(snap, prev)
                stats.append(compare_stats(t, snap, out))
                if not args.compare_only:
                    path = args.out / f"data/aws/{t:%Y/%m/%d/%H-%M-%S}.csv.gz"
                    writes.append(writer.submit(write_csv, path, out))
                    while len(writes) >= 64:             # backpressure: bound queued outputs
                        writes.popleft().result()
            if done % 1000 == 0:
                print(f"  {t} processed={done}/{len(slots)}", flush=True)
        for w in writes:
            w.result()

    st = pd.DataFrame(stats)
    st.to_csv(args.out / "validation.csv", index=False)
    print(f"done: written={len(st)} carried_without_query={carried} "
          f"query_without_snapshot={no_snapshot} duplicate_rows_dropped={dup_rows}")
    print(st.drop(columns="Time").sum().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
