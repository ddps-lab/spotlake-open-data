"""Upload the reprocessed 2025 snapshots (built by reprocess_2025.py) to the Open Data bucket.

Snapshots come either from a local build (<build>/data/aws/2025/...) or from a staging bucket
(s3://<source>/data/aws/2025/..., copied server-side) and go to s3://<dest>/data/aws/2025/...,
plus README.md and LICENSE.txt from the repository at the bucket root. Each object is a single
PUT or a single CopyObject, so its ETag is the MD5 of the content and --verify can compare
content, not only size.

The credentials used must be able to write to the destination bucket. When they belong to
another account (e.g. the SpotLake account), both sides must allow it:
  1. Destination bucket policy: add the statement from `--print-policy` to the policy the
     Open Data CloudFormation template created (do not replace it, or public read is lost),
     and remove it after the upload.
  2. The principal's own IAM policy allows s3:PutObject and s3:ListBucket on the bucket, and
     s3:GetObject on the staging bucket when copying from one.

Usage:
    python upload_to_open_data.py --dest spotlake-open-data --print-policy
    python upload_to_open_data.py --build ../build --dest spotlake-open-data            # dry run
    python upload_to_open_data.py --source-bucket spotlake-open-data-staging --dest spotlake-open-data
    python upload_to_open_data.py --source-bucket spotlake-open-data-staging --dest spotlake-open-data --execute
    python upload_to_open_data.py --source-bucket spotlake-open-data-staging --dest spotlake-open-data --verify
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


class LocalFile:
    def __init__(self, path):
        self.path = path
        self.size = path.stat().st_size

    def md5(self):
        return hashlib.md5(self.path.read_bytes()).hexdigest()

    def send(self, s3, bucket, key, ctype):
        s3.put_object(Bucket=bucket, Key=key, Body=self.path.read_bytes(), ContentType=ctype)


class SourceObject:
    """An object in the staging bucket. Its ETag is its MD5 because it was a single PUT under
    SSE-S3; the copy keeps that ETag as long as the destination also uses SSE-S3."""

    def __init__(self, bucket, key, size, etag):
        self.bucket, self.key, self.size, self.etag = bucket, key, size, etag

    def md5(self):
        return self.etag

    def send(self, s3, bucket, key, ctype):
        # REPLACE drops the source metadata and tags, so the copy needs no s3:PutObjectTagging.
        s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": self.bucket, "Key": self.key},
                       ContentType=ctype, MetadataDirective="REPLACE", TaggingDirective="REPLACE")


TOP_LEVEL_FILES = {"README.md": ("text/markdown", LocalFile(REPO / "README.md")),
                   "LICENSE.txt": ("text/plain", LocalFile(REPO / "LICENSE.txt"))}


def local_objects(build):
    root = build / PREFIX
    return {p.relative_to(root).as_posix(): LocalFile(p) for p in sorted(root.rglob("*.csv.gz"))}


def list_prefix(s3, bucket):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for o in page.get("Contents", []):
            yield o["Key"], o["Size"], o["ETag"].strip('"')


def source_objects(s3, bucket):
    objs = {}
    for key, size, etag in list_prefix(s3, bucket):
        if not key.endswith(".csv.gz"):
            continue
        if "-" in etag:
            raise SystemExit(f"{bucket}/{key}: multipart ETag, cannot verify by MD5")
        objs[key[len(PREFIX):]] = SourceObject(bucket, key, size, etag)
    return objs


def remote_objects(s3, bucket):
    return {key[len(PREFIX):]: (size, etag) for key, size, etag in list_prefix(s3, bucket)}


def same(item, remote_entry):
    return (remote_entry is not None and remote_entry[0] == item.size
            and remote_entry[1] == item.md5())


def verify_top_level(s3, bucket):
    ok = True
    for name, (_, item) in TOP_LEVEL_FILES.items():
        try:
            head = s3.head_object(Bucket=bucket, Key=name)
            entry = (head["ContentLength"], head["ETag"].strip('"'))
        except s3.exceptions.ClientError:
            entry = None
        if not same(item, entry):
            print(f"  top-level mismatch: {name}")
            ok = False
    return ok


def verify(items, remote, check_md5=True):
    missing = sorted(set(items) - set(remote))
    extra = sorted(set(remote) - set(items))
    bad = []
    for k in sorted(set(items) & set(remote)):
        size, etag = remote[k]
        if size != items[k].size or (check_md5 and etag != items[k].md5()):
            bad.append(k)
    print(f"source={len(items)} remote={len(remote)} missing={len(missing)} extra={len(extra)} mismatch={len(bad)}")
    for name, keys in (("missing", missing), ("extra", extra), ("mismatch", bad)):
        for k in keys[:10]:
            print(f"  {name}: {k}")
    return not (missing or extra or bad)


def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--build", type=Path, help="local build directory holding data/aws/2025/")
    src.add_argument("--source-bucket", help="staging bucket holding data/aws/2025/, copied server-side")
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
    if not (args.build or args.source_bucket):
        ap.error("one of --build or --source-bucket is required")

    s3 = session.client("s3", region_name=args.region,
                        config=Config(max_pool_connections=args.workers, retries={"mode": "adaptive"}))
    items = local_objects(args.build) if args.build else source_objects(s3, args.source_bucket)
    print(f"source snapshots={len(items)} bytes={sum(i.size for i in items.values())}")
    if not items:
        print("no snapshots in the source, aborting", file=sys.stderr)
        return 1

    remote = remote_objects(s3, args.dest)
    if args.verify:
        return 0 if verify(items, remote) and verify_top_level(s3, args.dest) else 1

    todo = [k for k, item in items.items() if not same(item, remote.get(k))]
    print(f"to upload={len(todo)} (identical already present={len(items) - len(todo)})")
    if not args.execute:
        print("dry run, pass --execute to upload")
        return 0

    for name, (ctype, item) in TOP_LEVEL_FILES.items():
        item.send(s3, args.dest, name, ctype)

    failed = []
    with ThreadPoolExecutor(args.workers) as pool:
        futures = {pool.submit(items[k].send, s3, args.dest, PREFIX + k, "application/gzip"): k for k in todo}
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
    ok = verify(items, remote_objects(s3, args.dest)) and verify_top_level(s3, args.dest)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
