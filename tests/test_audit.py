"""Tests for totali.audit.logger"""
import json
import tempfile
from pathlib import Path

import pytest
from totali.audit.logger import AuditLogger


@pytest.fixture
def audit(tmp_path):
    return AuditLogger(log_dir=str(tmp_path), project_id="test_project")


def test_log_creates_file(audit):
    audit.log("test_event", {"key": "value"})
    assert audit.log_path.exists()


def test_log_increments_sequence(audit):
    audit.log("event_1")
    audit.log("event_2")
    events = audit.get_events()
    assert events[0]["seq"] == 1
    assert events[1]["seq"] == 2


def test_log_stores_event_type(audit):
    audit.log("my_event", {"data": 123})
    events = audit.get_events()
    assert events[0]["event"] == "my_event"
    assert events[0]["data"]["data"] == 123


def test_chain_valid_on_clean_log(audit):
    audit.log("a")
    audit.log("b")
    audit.log("c")
    valid, errors = audit.verify_chain()
    assert valid is True
    assert errors == []


def test_chain_detects_tampering(audit):
    audit.log("a")
    audit.log("b")
    # Tamper with log
    with open(audit.log_path, "r") as f:
        lines = f.readlines()
    record = json.loads(lines[0])
    record["data"]["injected"] = True
    lines[0] = json.dumps(record) + "\n"
    with open(audit.log_path, "w") as f:
        f.writelines(lines)
    valid, errors = audit.verify_chain()
    assert valid is False
    assert len(errors) > 0


def test_get_events_filtered(audit):
    audit.log("alpha", {"v": 1})
    audit.log("beta", {"v": 2})
    audit.log("alpha", {"v": 3})
    alphas = audit.get_events("alpha")
    assert len(alphas) == 2
    betas = audit.get_events("beta")
    assert len(betas) == 1


def test_get_events_unfiltered(audit):
    audit.log("a")
    audit.log("b")
    assert len(audit.get_events()) == 2


def test_summary(audit):
    audit.log("a")
    audit.log("a")
    audit.log("b")
    s = audit.summary()
    assert s["project_id"] == "test_project"
    assert s["total_events"] == 3
    assert s["event_counts"]["a"] == 2
    assert s["event_counts"]["b"] == 1
    assert s["chain_valid"] is True


def test_genesis_block_hash(audit):
    audit.log("first")
    events = audit.get_events()
    assert events[0]["prev_hash"] == "0" * 64


def test_empty_log_valid(audit):
    valid, errors = audit.verify_chain()
    assert valid is True
