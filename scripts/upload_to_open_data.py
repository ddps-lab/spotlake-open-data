"""Upload the reprocessed 2025 snapshots (built by reprocess_2025.py) to the Open Data bucket.

Local <build>/data/aws/2025/... -> s3://<dest>/data/aws/2025/..., plus README.md and LICENSE.txt
at the bucket root. Each object is a single PUT, so its ETag is the MD5 of the file and
--verify can compare content, not only size.

The credentials used must be able to write to the destination bucket. When they belong to
another account (e.g. the SpotLake account), both sides must allow it:
  1. Destination bucket policy: add the statement from `--print-policy` to the policy the
     Open Data CloudFormation template created (do not replace it, or public read is lost),
     and remove it after the upload.
  2. The principal's own IAM policy allows s3:PutObject and s3:ListBucket on the bucket.

Usage:
    python upload_to_open_data.py --build ../build --dest spotlake-open-data --print-policy
    python upload_to_open_data.py --build ../build --dest spotlake-open-data            # dry run
    python upload_to_open_data.py --build ../build --dest spotlake-open-data --execute
    python upload_to_open_data.py --build ../build --dest spotlake-open-data --verify
"""
import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config

PREFIX = "data/aws/2025/"
REPO = Path(__file__).resolve().parent.parent
TOP_LEVEL_FILES = {"README.md": ("text/markdown", REPO / "README.md"),
                   "LICENSE.txt": ("text/plain", REPO / "LICENSE.txt")}


def local_objects(build):
    root = build / PREFIX
    return {p.relative_to(root).as_posix(): p for p in sorted(root.rglob("*.csv.gz"))}


def md5(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


def remote_objects(s3, bucket):
    objs = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for o in page.get("Contents", []):
            objs[o["Key"][len(PREFIX):]] = (o["Size"], o["ETag"].strip('"'))
    return objs


def same(path, remote_entry):
    return (remote_entry is not None and remote_entry[0] == path.stat().st_size
            and remote_entry[1] == md5(path))


def verify_top_level(s3, bucket):
    ok = True
    for name, (_, path) in TOP_LEVEL_FILES.items():
        try:
            head = s3.head_object(Bucket=bucket, Key=name)
            entry = (head["ContentLength"], head["ETag"].strip('"'))
        except s3.exceptions.ClientError:
            entry = None
        if not same(path, entry):
            print(f"  top-level mismatch: {name}")
            ok = False
    return ok


def put(s3, bucket, key, path, ctype):
    s3.put_object(Bucket=bucket, Key=key, Body=path.read_bytes(), ContentType=ctype)


def verify(local, remote, check_md5=True):
    missing = sorted(set(local) - set(remote))
    extra = sorted(set(remote) - set(local))
    bad = []
    for k in sorted(set(local) & set(remote)):
        size, etag = remote[k]
        if size != local[k].stat().st_size or (check_md5 and etag != md5(local[k])):
            bad.append(k)
    print(f"local={len(local)} remote={len(remote)} missing={len(missing)} extra={len(extra)} mismatch={len(bad)}")
    for name, keys in (("missing", missing), ("extra", extra), ("mismatch", bad)):
        for k in keys[:10]:
            print(f"  {name}: {k}")
    return not (missing or extra or bad)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", required=True, type=Path)
    ap.add_argument("--dest", required=True)
    ap.add_argument("--profile", help="AWS profile; default uses the environment or instance role")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--workers", type=int, default=32)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--print-policy", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--verify", action="store_true")
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile)
    if args.print_policy:
        arn = session.client("sts").get_caller_identity()["Arn"]
        print(json.dumps({
            "Sid": "TemporarySpotLakeUpload",
            "Effect": "Allow",
            "Principal": {"AWS": arn},
            "Action": ["s3:PutObject", "s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{args.dest}", f"arn:aws:s3:::{args.dest}/*"],
        }, indent=2))
        return 0

    local = local_objects(args.build)
    print(f"local snapshots={len(local)} bytes={sum(p.stat().st_size for p in local.values())}")
    if not local:
        print("no snapshots under the build directory, aborting", file=sys.stderr)
        return 1

    s3 = session.client("s3", region_name=args.region,
                        config=Config(max_pool_connections=args.workers, retries={"mode": "adaptive"}))
    remote = remote_objects(s3, args.dest)
    if args.verify:
        return 0 if verify(local, remote) and verify_top_level(s3, args.dest) else 1

    todo = [k for k, p in local.items() if not same(p, remote.get(k))]
    print(f"to upload={len(todo)} (identical already present={len(local) - len(todo)})")
    if not args.execute:
        print("dry run, pass --execute to upload")
        return 0

    for name, (ctype, path) in TOP_LEVEL_FILES.items():
        put(s3, args.dest, name, path, ctype)

    failed = []
    with ThreadPoolExecutor(args.workers) as pool:
        futures = {pool.submit(put, s3, args.dest, PREFIX + k, local[k], "application/gzip"): k for k in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                fut.result()
            except Exception as e:  # keep going, report at the end
                failed.append((futures[fut], e))
            if i % 2000 == 0:
                print(f"  {i}/{len(todo)}")
    print(f"uploaded={len(todo) - len(failed)} failed={len(failed)}")
    for k, e in failed[:10]:
        print(f"  {k}: {e}")
    if failed:
        return 1
    ok = verify(local, remote_objects(s3, args.dest)) and verify_top_level(s3, args.dest)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
