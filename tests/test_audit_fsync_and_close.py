"""A-7: fsync on every write + terminal `run_end` record on close."""

import json

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log


class TestFsync:
    def test_written_bytes_visible_without_explicit_flush(self, tmp_path):
        """Durability contract: a record is readable immediately after log()."""
        logger = AuditLogger(log_dir=str(tmp_path), project_id="fs")
        logger.log("event", {"k": 1})

        raw = logger.log_path.read_text()
        assert raw.count("\n") >= 1
        record = json.loads(raw.splitlines()[0])
        assert record["event"] == "event"
        assert record["data"]["k"] == 1

    def test_chain_valid_without_close(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="nc")
        logger.log("a", {})
        logger.log("b", {})
        ok, errors = verify_log(logger.log_path)
        assert ok is True, errors


class TestClose:
    def test_close_writes_run_end(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="ce")
        logger.log("ingest", {})
        logger.close()

        events = logger.get_events()
        assert events[-1]["event"] == "run_end"
        # seq_total captures the count of events before run_end; the run_end
        # record itself gets seq = seq_total + 1.
        assert events[-1]["data"]["seq_total"] == 1
        assert events[-1]["seq"] == 2

    def test_close_is_idempotent(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="idem")
        logger.log("x", {})
        logger.close()
        before = logger.log_path.read_text()
        logger.close()
        after = logger.log_path.read_text()
        assert before == after, "second close must be a no-op"

    def test_close_preserves_chain(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="chain")
        for i in range(5):
            logger.log("e", {"i": i})
        logger.close()
        ok, errors = verify_log(logger.log_path)
        assert ok is True, errors

    def test_close_with_summary(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="sum")
        logger.log("e", {})
        logger.close({"phases_run": 3, "ok": True})

        events = logger.get_events("run_end")
        assert events[0]["data"]["summary"] == {"phases_run": 3, "ok": True}

    def test_run_end_without_prior_events(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="solo")
        logger.close()
        events = logger.get_events()
        assert len(events) == 1
        assert events[0]["event"] == "run_end"
        # No prior events → seq_total = 0 (snapshot before run_end's own
        # increment bumps the sequence).
        assert events[0]["data"]["seq_total"] == 0
        assert events[0]["seq"] == 1
