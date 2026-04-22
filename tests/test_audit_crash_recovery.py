"""A-7 partial: crash-recovery semantics.

A crash mid-write produces a detectable state: either a fully-formed JSONL
record lands (persistent open-append fsync behavior) or a partial line remains
that the verifier reports. Silent repair is forbidden.
"""

from totali.audit.logger import AuditLogger
from totali.audit.verify import verify_log


class TestPartialLine:
    def test_trailing_partial_detected(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="partial")
        logger.log("a", {})
        logger.log("b", {})
        # Simulate a crash after a partial write by truncating the last char.
        data = logger.log_path.read_text()
        logger.log_path.write_text(data[:-5])  # drop trailing chars inside the line

        ok, errors = verify_log(logger.log_path)
        assert ok is False
        assert errors, "verifier must surface partial/garbled record"

    def test_truncated_mid_payload_detected(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="midp")
        logger.log("x", {"big": "x" * 100})
        data = logger.log_path.read_text()
        # Chop the middle of the single line.
        logger.log_path.write_text(data[: len(data) // 2])

        ok, errors = verify_log(logger.log_path)
        assert ok is False
        assert errors


class TestRecoveryBehavior:
    def test_new_logger_does_not_touch_old_file(self, tmp_path):
        """Opening a new logger with the same project does not mutate existing logs."""
        l1 = AuditLogger(log_dir=str(tmp_path), project_id="same")
        l1.log("ingest", {})
        path1 = l1.log_path
        content_before = path1.read_text()

        l2 = AuditLogger(log_dir=str(tmp_path), project_id="same")
        l2.log("ingest", {})

        # Even if timestamps collide, l2 writes to its own path or l1's
        # existing content remains readable.
        assert path1.exists()
        if l2.log_path == path1:
            # same-second open: both appended, but l1's bytes are still a
            # prefix of the current content (append-only).
            assert path1.read_text().startswith(content_before)
        else:
            assert path1.read_text() == content_before

    def test_write_survives_logger_discard(self, tmp_path):
        """Writes are immediately durable (not buffered to logger destruction)."""
        logger = AuditLogger(log_dir=str(tmp_path), project_id="durable")
        logger.log("event", {"k": "v"})
        path = logger.log_path
        del logger

        ok, errors = verify_log(path)
        assert ok is True
        assert errors == []


class TestNoSilentRepair:
    def test_verify_does_not_rewrite(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="noed")
        logger.log("a", {})
        before_bytes = logger.log_path.read_bytes()

        _ = verify_log(logger.log_path)

        after_bytes = logger.log_path.read_bytes()
        assert before_bytes == after_bytes, "verify_log must be read-only"
