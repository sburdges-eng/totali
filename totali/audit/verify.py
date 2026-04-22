"""Audit chain verifier CLI.

Walks a JSONL audit log and confirms the SHA-256 hash chain. Exits 0 on a
clean chain, 3 on tampering or structural corruption. Intended for:

- Global gate #10 in AGENTIC_COMPLETION_PLAN.md §5
- Manual spot-checks on audit_logs/ before deliverable handoff
- CI verification after a pipeline run

Usage:
    python -m totali.audit.verify <path-to-log.jsonl>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def verify_log(path: Path, hash_algo: str = "sha256") -> tuple[bool, list[str]]:
    """Verify the hash chain of a single JSONL audit log.

    Returns (ok, errors). Errors carry line numbers and a short description.
    """
    if not path.exists():
        return False, [f"log file does not exist: {path}"]

    errors: list[str] = []
    prev_hash = "0" * 64

    with path.open("r", encoding="utf-8") as f:
        for line_num, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as e:
                errors.append(f"line {line_num}: JSON parse error: {e}")
                continue

            try:
                stored_hash = record.pop("hash")
            except KeyError:
                errors.append(f"line {line_num}: missing 'hash' field")
                continue

            declared_prev = record.get("prev_hash")
            if declared_prev != prev_hash:
                errors.append(
                    f"line {line_num}: prev_hash mismatch "
                    f"(expected {prev_hash[:16]}..., got "
                    f"{(declared_prev or '')[:16]}...)"
                )

            record_bytes = json.dumps(
                record, sort_keys=True, default=str
            ).encode("utf-8")
            computed = hashlib.new(hash_algo, record_bytes).hexdigest()
            if computed != stored_hash:
                errors.append(
                    f"line {line_num}: hash mismatch "
                    f"(computed {computed[:16]}..., stored {stored_hash[:16]}...)"
                )

            prev_hash = stored_hash

    return len(errors) == 0, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="totali.audit.verify",
        description="Verify the SHA-256 hash chain of a TOTaLi audit log.",
    )
    parser.add_argument("log_path", type=Path, help="Path to the JSONL audit log")
    parser.add_argument(
        "--hash-algo",
        default="sha256",
        help="Hash algorithm (default: sha256)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-error output; only set the exit code",
    )
    args = parser.parse_args(argv)

    ok, errors = verify_log(args.log_path, hash_algo=args.hash_algo)
    if ok:
        if not args.quiet:
            print(f"OK: {args.log_path} (chain verified)")
        return 0
    if not args.quiet:
        print(f"FAIL: {args.log_path}")
        for e in errors:
            print(f"  - {e}")
    return 3


if __name__ == "__main__":
    sys.exit(main())
