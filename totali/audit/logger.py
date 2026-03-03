"""
Audit Logger – Chain of Custody
================================
Every pipeline action is logged as a JSONL event with SHA-256 chaining.
Supports later disputes and reproducibility verification.
"""

import json
import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class AuditLogger:
    def __init__(
        self,
        log_dir: str = "audit_logs",
        project_id: str = "unknown",
        hash_algo: str = "sha256",
    ):
        # Validate project_id to prevent path traversal
        if not re.match(r"^[a-zA-Z0-9_-]+$", project_id):
            raise ValueError(f"Invalid project_id: {project_id}. Only alphanumeric, underscores, and dashes allowed.")

        self.log_dir = Path(log_dir).resolve()

        # Security: Ensure log_dir exists with restrictive permissions
        if not self.log_dir.exists():
            self.log_dir.mkdir(parents=True, exist_ok=True)
            # Standard directory permissions: 0o700 (owner only)
            os.chmod(self.log_dir, 0o700)

        self.project_id = project_id
        self.hash_algo = hash_algo

        filename = f"{project_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        self.log_path = (self.log_dir / filename).resolve()

        # Security: Final check that log_path is within log_dir
        if not str(self.log_path).startswith(str(self.log_dir)):
            raise ValueError(f"Insecure log path generated: {self.log_path}")

        self._prev_hash = "0" * 64  # genesis block
        self._seq = 0

    def log(self, event_type: str, data: Optional[dict] = None):
        """Log an auditable event with hash chaining."""
        self._seq += 1
        timestamp = datetime.now(timezone.utc).isoformat()

        record = {
            "seq": self._seq,
            "timestamp": timestamp,
            "project_id": self.project_id,
            "event": event_type,
            "data": data or {},
            "prev_hash": self._prev_hash,
        }

        # Compute hash of this record (chain of custody)
        record_bytes = json.dumps(record, sort_keys=True, default=str).encode()
        record_hash = hashlib.new(self.hash_algo, record_bytes).hexdigest()
        record["hash"] = record_hash
        self._prev_hash = record_hash

        # Security: Ensure log file has restrictive permissions
        # If it doesn't exist, it will be created with default mask.
        # We'll set it to 0o600 (owner only) if we create it.
        file_exists = self.log_path.exists()

        # Append to JSONL
        with open(self.log_path, "a") as f:
            if not file_exists:
                os.chmod(self.log_path, 0o600)
            f.write(json.dumps(record, default=str) + "\n")

    def verify_chain(self) -> tuple[bool, list]:
        """Verify the integrity of the audit log hash chain."""
        if not self.log_path.exists():
            return True, []

        errors = []
        prev_hash = "0" * 64

        with open(self.log_path, "r") as f:
            for line_num, line in enumerate(f, 1):
                record = json.loads(line.strip())
                stored_hash = record.pop("hash")

                # Verify prev_hash links
                if record["prev_hash"] != prev_hash:
                    errors.append(
                        f"Line {line_num}: prev_hash mismatch "
                        f"(expected {prev_hash[:16]}..., got {record['prev_hash'][:16]}...)"
                    )

                # Verify record hash
                record_bytes = json.dumps(record, sort_keys=True, default=str).encode()
                computed = hashlib.new(self.hash_algo, record_bytes).hexdigest()
                if computed != stored_hash:
                    errors.append(
                        f"Line {line_num}: hash mismatch "
                        f"(computed {computed[:16]}..., stored {stored_hash[:16]}...)"
                    )

                prev_hash = stored_hash

        return len(errors) == 0, errors

    def get_events(self, event_type: Optional[str] = None) -> list:
        """Read back events, optionally filtered by type."""
        if not self.log_path.exists():
            return []

        events = []
        with open(self.log_path, "r") as f:
            for line in f:
                record = json.loads(line.strip())
                if event_type is None or record["event"] == event_type:
                    events.append(record)

        return events

    def summary(self) -> dict:
        """Get audit log summary."""
        events = self.get_events()
        event_counts = {}
        for e in events:
            event_counts[e["event"]] = event_counts.get(e["event"], 0) + 1

        return {
            "project_id": self.project_id,
            "log_path": str(self.log_path),
            "total_events": len(events),
            "event_counts": event_counts,
            "first_event": events[0]["timestamp"] if events else None,
            "last_event": events[-1]["timestamp"] if events else None,
            "chain_valid": self.verify_chain()[0],
        }
