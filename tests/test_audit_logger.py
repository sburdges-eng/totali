"""Tests for AuditLogger hash-chaining and verification."""

import json
import os
import pytest
from pathlib import Path
from totali.audit.logger import AuditLogger


class TestAuditLoggerBasics:
    def test_creates_log_directory(self, tmp_path):
        log_dir = tmp_path / "new_audit"
        AuditLogger(log_dir=str(log_dir), project_id="p1")
        assert log_dir.exists()
        # Check permissions (0700)
        mode = os.stat(log_dir).st_mode & 0o777
        assert mode == 0o700

    def test_log_creates_file(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("test_event", {"key": "value"})
        assert logger.log_path.exists()
        # Check file permissions (0600)
        mode = os.stat(logger.log_path).st_mode & 0o777
        assert mode == 0o600

    def test_log_writes_jsonl(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("event_a", {"x": 1})
        logger.log("event_b", {"x": 2})

        lines = logger.log_path.read_text().strip().split("\n")
        assert len(lines) == 2

        r1 = json.loads(lines[0])
        r2 = json.loads(lines[1])
        assert r1["event"] == "event_a"
        assert r2["event"] == "event_b"
        assert r1["seq"] == 1
        assert r2["seq"] == 2

    def test_log_includes_required_fields(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="proj")
        logger.log("ingest", {"file": "test.las"})

        record = json.loads(logger.log_path.read_text().strip())
        assert "seq" in record
        assert "timestamp" in record
        assert "project_id" in record
        assert "event" in record
        assert "data" in record
        assert "prev_hash" in record
        assert "hash" in record

    def test_log_without_data(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("empty_event")
        record = json.loads(logger.log_path.read_text().strip())
        assert record["data"] == {}


class TestHashChaining:
    def test_genesis_block_prev_hash(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("first")

        record = json.loads(logger.log_path.read_text().strip())
        assert record["prev_hash"] == "0" * 64

    def test_chain_links_correctly(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("a")
        logger.log("b")
        logger.log("c")

        lines = logger.log_path.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines]

        assert records[0]["prev_hash"] == "0" * 64
        assert records[1]["prev_hash"] == records[0]["hash"]
        assert records[2]["prev_hash"] == records[1]["hash"]

    def test_verify_chain_valid(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("x", {"a": 1})
        logger.log("y", {"b": 2})
        logger.log("z", {"c": 3})

        valid, errors = logger.verify_chain()
        assert valid is True
        assert errors == []

    def test_verify_chain_detects_tampering(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("a")
        logger.log("b")

        # Tamper with the log
        lines = logger.log_path.read_text().strip().split("\n")
        record = json.loads(lines[0])
        record["data"]["injected"] = "evil"
        lines[0] = json.dumps(record, default=str)
        logger.log_path.write_text("\n".join(lines) + "\n")

        valid, errors = logger.verify_chain()
        assert valid is False
        assert len(errors) > 0

    def test_verify_empty_log(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        # Don't log anything — file doesn't exist
        valid, errors = logger.verify_chain()
        assert valid is True


class TestEventRetrieval:
    def test_get_all_events(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("ingest")
        logger.log("classify")
        logger.log("extract")

        events = logger.get_events()
        assert len(events) == 3

    def test_get_filtered_events(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        logger.log("ingest")
        logger.log("classify")
        logger.log("ingest")

        events = logger.get_events("ingest")
        assert len(events) == 2
        assert all(e["event"] == "ingest" for e in events)

    def test_get_events_empty_log(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="p1")
        assert logger.get_events() == []


class TestSummary:
    def test_summary_structure(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="test")
        logger.log("a")
        logger.log("b")
        logger.log("a")

        s = logger.summary()
        assert s["project_id"] == "test"
        assert s["total_events"] == 3
        assert s["event_counts"]["a"] == 2
        assert s["event_counts"]["b"] == 1
        assert s["chain_valid"] is True
        assert s["first_event"] is not None
        assert s["last_event"] is not None

class TestAuditSecurity:
    def test_project_id_validation(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid project_id"):
            AuditLogger(log_dir=str(tmp_path), project_id="../evil")
