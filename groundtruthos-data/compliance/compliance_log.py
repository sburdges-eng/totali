"""
Dataset compliance logging and audit trail.

Every dataset acquisition is logged with license verification,
integrity checks, and approval status for legal defensibility.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fcntl

logger = logging.getLogger(__name__)

DEFAULT_LOG_PATH = Path("compliance_log.jsonl")
SCHEMA_VERSION = "1.0.0"

ALLOWED_APPROVAL_STATUSES = {
    "pending",
    "approved_training",
    "approved_research",
    "rejected",
}

ALLOWED_RISK_LEVELS = {"low", "medium", "high", "critical"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_date_from_iso(timestamp_iso: str) -> str:
    return timestamp_iso.split("T", 1)[0]


def _is_absolute_path(path_str: str) -> bool:
    p = Path(path_str)
    if p.is_absolute():
        return True
    # Windows drive-letter style absolute path support.
    if len(path_str) >= 3 and path_str[1] == ":" and path_str[2] in ("/", "\\"):
        return True
    return False


def _normalize_dataset_relative_path(file_path: str) -> str:
    normalized = file_path.strip().replace("\\", "/")
    if not normalized:
        raise ValueError("file_path must be non-empty")
    if _is_absolute_path(normalized):
        raise ValueError("file_path must be dataset-relative or repo-relative, not absolute")
    if normalized.startswith("../") or normalized == "..":
        raise ValueError("file_path may not escape root via '..'")
    return normalized


class ComplianceLogger:
    """Append-only compliance log for dataset acquisitions."""

    def __init__(self, log_path: Path = DEFAULT_LOG_PATH):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_acquisition(
        self,
        source: str,
        tile_id: str,
        file_path: str,
        license_info: dict,
        integrity: dict | None = None,
        approval_status: str = "pending",
        approved_by: str = "",
        notes: str = "",
        *,
        collection_method: str,
        hash_manifest: dict,
        compression_format: str,
        shard_checksums: list[str],
        add_hash_chain: bool = True,
    ):
        """Log a dataset acquisition event.

        Args:
            source: Dataset source identifier.
            tile_id: Tile/file identifier.
            file_path: Dataset-relative or repo-relative path.
            license_info: License metadata dict.
            integrity: Integrity check results dict.
            approval_status: One of: pending, approved_training, approved_research, rejected.
            approved_by: Name/ID of approver.
            notes: Additional notes.
            collection_method: How the dataset was collected.
            hash_manifest: Immutable hash manifest metadata.
            compression_format: Compression format identifier.
            shard_checksums: Deterministic shard checksum list.
            add_hash_chain: Include prevRecordHash and recordHash fields.
        """
        timestamp = _utc_now_iso()
        normalized_path = _normalize_dataset_relative_path(file_path)

        self._require_non_empty("source", source)
        self._require_non_empty("tile_id", tile_id)
        self._require_non_empty("collection_method", collection_method)
        self._require_non_empty("compression_format", compression_format)
        self._validate_license_info(license_info)
        self._validate_approval_status(approval_status)

        if not isinstance(hash_manifest, dict) or not hash_manifest:
            raise ValueError("hash_manifest must be a non-empty dict")
        if not isinstance(shard_checksums, list) or not shard_checksums:
            raise ValueError("shard_checksums must be a non-empty list")
        if not all(isinstance(item, str) and item.strip() for item in shard_checksums):
            raise ValueError("shard_checksums must contain only non-empty strings")
        if integrity is not None and not isinstance(integrity, dict):
            raise ValueError("integrity must be a dict when provided")

        record = {
            "schemaVersion": SCHEMA_VERSION,
            "event": "acquisition",
            "timestamp": timestamp,
            "canonicalOrdering": [
                "schemaVersion",
                "event",
                "timestamp",
                "source",
                "tileId",
                "filePath",
                "license",
                "collectionMethod",
                "hashManifest",
                "compressionFormat",
                "shardChecksums",
                "integrity",
                "approval",
                "notes",
                "invariants",
                "prevRecordHash",
                "recordHash",
            ],
            "invariants": [
                "append_only",
                "deterministic_json",
                "file_path_relative",
                "approval_status_enum",
                "non_empty_hash_manifest",
                "non_empty_shard_checksums",
            ],
            "source": source,
            "tileId": tile_id,
            "filePath": normalized_path,
            "license": {
                "type": license_info.get("license", "unknown"),
                "commercialUse": license_info.get("commercial_use"),
                "attributionRequired": license_info.get("attribution_required", False),
                "redistribution": license_info.get("redistribution"),
            },
            "collectionMethod": collection_method,
            "hashManifest": hash_manifest,
            "compressionFormat": compression_format,
            "shardChecksums": shard_checksums,
            "integrity": integrity or {},
            "approval": {
                "status": approval_status,
                "approvedBy": approved_by,
                "date": _utc_date_from_iso(timestamp),
            },
            "notes": notes,
        }

        self._validate_record(record)
        self._append(record, add_hash_chain=add_hash_chain)

    def log_license_review(
        self,
        source: str,
        license_type: str,
        commercial_use: bool,
        redistribution: bool,
        attribution_required: bool,
        reviewed_by: str,
        risk_level: str = "low",
        notes: str = "",
        *,
        add_hash_chain: bool = True,
    ):
        """Log a license review event for a data source."""
        timestamp = _utc_now_iso()
        self._require_non_empty("source", source)
        self._require_non_empty("license_type", license_type)
        self._require_non_empty("reviewed_by", reviewed_by)
        if risk_level not in ALLOWED_RISK_LEVELS:
            raise ValueError(f"risk_level must be one of: {sorted(ALLOWED_RISK_LEVELS)}")

        record = {
            "schemaVersion": SCHEMA_VERSION,
            "event": "license_review",
            "timestamp": timestamp,
            "canonicalOrdering": [
                "schemaVersion",
                "event",
                "timestamp",
                "source",
                "licenseType",
                "commercialUse",
                "redistribution",
                "attributionRequired",
                "reviewedBy",
                "riskLevel",
                "notes",
                "invariants",
                "prevRecordHash",
                "recordHash",
            ],
            "invariants": ["append_only", "deterministic_json", "risk_level_enum"],
            "source": source,
            "licenseType": license_type,
            "commercialUse": bool(commercial_use),
            "redistribution": bool(redistribution),
            "attributionRequired": bool(attribution_required),
            "reviewedBy": reviewed_by,
            "riskLevel": risk_level,
            "notes": notes,
        }

        self._validate_record(record)
        self._append(record, add_hash_chain=add_hash_chain)

    def log_rejection(
        self,
        source: str,
        tile_id: str,
        reason: str,
        rejected_by: str = "automated",
        *,
        add_hash_chain: bool = True,
    ):
        """Log a dataset rejection."""
        timestamp = _utc_now_iso()
        self._require_non_empty("source", source)
        self._require_non_empty("tile_id", tile_id)
        self._require_non_empty("reason", reason)

        record = {
            "schemaVersion": SCHEMA_VERSION,
            "event": "rejection",
            "timestamp": timestamp,
            "canonicalOrdering": [
                "schemaVersion",
                "event",
                "timestamp",
                "source",
                "tileId",
                "reason",
                "rejectedBy",
                "invariants",
                "prevRecordHash",
                "recordHash",
            ],
            "invariants": ["append_only", "deterministic_json"],
            "source": source,
            "tileId": tile_id,
            "reason": reason,
            "rejectedBy": rejected_by,
        }

        self._validate_record(record)
        self._append(record, add_hash_chain=add_hash_chain)

    def _append(self, record: dict[str, Any], *, add_hash_chain: bool):
        """Append a validated record to the log file with file locking."""
        with open(self.log_path, "a+", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0)
                prev_hash = self._load_last_record_hash_from_handle(f)
                if add_hash_chain:
                    record["prevRecordHash"] = prev_hash
                    record["recordHash"] = self._compute_record_hash(record)
                payload = self._serialize_deterministic(record)
                f.seek(0, 2)
                f.write(payload + "\n")
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _load_last_record_hash_from_handle(self, handle) -> str:
        handle.seek(0)
        last_non_empty = ""
        for line in handle:
            line = line.strip()
            if line:
                last_non_empty = line
        if not last_non_empty:
            return ""
        try:
            last_record = json.loads(last_non_empty)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed compliance log tail: {exc}") from exc
        return str(last_record.get("recordHash", ""))

    def _serialize_deterministic(self, record: dict[str, Any]) -> str:
        return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    def _compute_record_hash(self, record: dict[str, Any]) -> str:
        payload_without_hash = dict(record)
        payload_without_hash.pop("recordHash", None)
        canonical = self._serialize_deterministic(payload_without_hash)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def read_log(self) -> list[dict[str, Any]]:
        """Read all log entries with strict validation (fail closed)."""
        if not self.log_path.exists():
            return []

        entries: list[dict[str, Any]] = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                for line_no, raw in enumerate(f, start=1):
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"Malformed JSON at line {line_no}: {exc}") from exc
                    self._validate_record(entry)
                    entries.append(entry)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        return entries

    def get_source_summary(self) -> dict[str, dict[str, Any]]:
        """Summarize compliance status by source."""
        entries = self.read_log()
        summary: dict[str, dict[str, Any]] = {}
        for entry in entries:
            source = entry.get("source", "unknown")
            if source not in summary:
                summary[source] = {
                    "acquisitions": 0,
                    "rejections": 0,
                    "license_reviewed": False,
                    "risk_level": "unknown",
                }
            if entry["event"] == "acquisition":
                summary[source]["acquisitions"] += 1
            elif entry["event"] == "rejection":
                summary[source]["rejections"] += 1
            elif entry["event"] == "license_review":
                summary[source]["license_reviewed"] = True
                summary[source]["risk_level"] = entry.get("riskLevel", "unknown")

        return summary

    def _validate_record(self, record: dict[str, Any]) -> None:
        if not isinstance(record, dict):
            raise ValueError("Record must be an object")
        if record.get("schemaVersion") != SCHEMA_VERSION:
            raise ValueError(f"schemaVersion must be '{SCHEMA_VERSION}'")
        if "event" not in record or not isinstance(record["event"], str):
            raise ValueError("event must be a string")
        if "timestamp" not in record or not isinstance(record["timestamp"], str):
            raise ValueError("timestamp must be a string")
        if "canonicalOrdering" not in record or not isinstance(record["canonicalOrdering"], list):
            raise ValueError("canonicalOrdering must be a list")
        if "invariants" not in record or not isinstance(record["invariants"], list):
            raise ValueError("invariants must be a list")

        event = record["event"]
        if event == "acquisition":
            for field in [
                "source",
                "tileId",
                "filePath",
                "license",
                "collectionMethod",
                "hashManifest",
                "compressionFormat",
                "shardChecksums",
                "approval",
            ]:
                if field not in record:
                    raise ValueError(f"acquisition missing required field: {field}")
            if _is_absolute_path(str(record["filePath"])):
                raise ValueError("filePath must be relative")
            if not isinstance(record["hashManifest"], dict) or not record["hashManifest"]:
                raise ValueError("hashManifest must be a non-empty object")
            if not isinstance(record["shardChecksums"], list) or not record["shardChecksums"]:
                raise ValueError("shardChecksums must be a non-empty list")
            approval = record["approval"]
            if not isinstance(approval, dict):
                raise ValueError("approval must be an object")
            status = approval.get("status")
            self._validate_approval_status(status)

        elif event == "license_review":
            for field in [
                "source",
                "licenseType",
                "commercialUse",
                "redistribution",
                "attributionRequired",
                "reviewedBy",
                "riskLevel",
            ]:
                if field not in record:
                    raise ValueError(f"license_review missing required field: {field}")
            risk = record["riskLevel"]
            if risk not in ALLOWED_RISK_LEVELS:
                raise ValueError(f"riskLevel must be one of: {sorted(ALLOWED_RISK_LEVELS)}")

        elif event == "rejection":
            for field in ["source", "tileId", "reason", "rejectedBy"]:
                if field not in record:
                    raise ValueError(f"rejection missing required field: {field}")

        else:
            raise ValueError(f"Unsupported event: {event}")

    def _validate_license_info(self, license_info: dict[str, Any]) -> None:
        if not isinstance(license_info, dict):
            raise ValueError("license_info must be an object")
        if "license" not in license_info:
            raise ValueError("license_info.license is required")

    def _validate_approval_status(self, approval_status: str) -> None:
        if approval_status not in ALLOWED_APPROVAL_STATUSES:
            raise ValueError(
                f"approval_status must be one of: {sorted(ALLOWED_APPROVAL_STATUSES)}"
            )

    def _require_non_empty(self, field: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")


# Pre-approved sources that can be auto-approved
AUTO_APPROVED_SOURCES = {
    "usgs_3dep": {
        "license": "Public Domain",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "US federal government work, 17 USC 105",
    },
    "usgs_dem": {
        "license": "Public Domain",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "US federal government work, 17 USC 105",
    },
    "noaa_digital_coast": {
        "license": "Public Domain",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "US federal government work, 17 USC 105",
    },
    "netherlands_ahn4": {
        "license": "CC0",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "CC0 public domain dedication",
    },
    "ssurgo": {
        "license": "Public Domain",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "US federal government work (USDA)",
    },
    "gssurgo": {
        "license": "Public Domain",
        "commercial_use": True,
        "attribution_required": False,
        "redistribution": True,
        "risk_level": "low",
        "reason": "US federal government work (USDA)",
    },
    "copernicus_dem": {
        "license": "Copernicus License",
        "commercial_use": True,
        "attribution_required": True,
        "redistribution": True,
        "risk_level": "low",
        "reason": "Free and open access per Copernicus data policy",
    },
}

# Sources requiring manual license review before ingestion
REQUIRES_REVIEW_SOURCES = {
    "opentopography": "License varies per dataset — must check individually",
    "uk_ea_lidar": "OGL v3 — generally safe but verify attribution requirements",
    "soilgrids": "CC-BY 4.0 — commercial OK but attribution required",
}

# Blocked sources — do not ingest
BLOCKED_SOURCES = {
    "alos_aw3d30": "JAXA license requires separate commercial agreement",
}


def check_source_compliance(source: str) -> tuple[str, str]:
    """Check if a source is auto-approved, needs review, or is blocked.

    Returns:
        Tuple of (status, reason) where status is one of:
        'auto_approved', 'requires_review', 'blocked'
    """
    if source in AUTO_APPROVED_SOURCES:
        info = AUTO_APPROVED_SOURCES[source]
        return "auto_approved", info["reason"]
    if source in REQUIRES_REVIEW_SOURCES:
        return "requires_review", REQUIRES_REVIEW_SOURCES[source]
    if source in BLOCKED_SOURCES:
        return "blocked", BLOCKED_SOURCES[source]
    return "requires_review", "Unknown source — manual license review required"
