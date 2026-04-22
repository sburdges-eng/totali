"""A-5: Audit event allowlist enforcement.

Allowlist is opt-in (None = legacy unrestricted behavior, preserved). When
configured, emit of an event outside the list raises `UnknownAuditEvent` and
no record is written.
"""

import pytest

from totali.audit.logger import AuditLogger, UnknownAuditEvent


class TestAllowlistOptIn:
    def test_default_is_unrestricted(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="default")
        logger.log("any_event_at_all", {"x": 1})
        events = logger.get_events()
        assert len(events) == 1
        assert events[0]["event"] == "any_event_at_all"

    def test_allowlist_accepts_listed_events(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="al",
            allowed_events={"ingest", "transform", "classify"},
        )
        logger.log("ingest", {})
        logger.log("transform", {})
        logger.log("classify", {})
        assert len(logger.get_events()) == 3

    def test_allowlist_rejects_unknown(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="reject",
            allowed_events={"ingest"},
        )
        with pytest.raises(UnknownAuditEvent) as exc:
            logger.log("not_listed_event", {})
        assert "not_listed_event" in str(exc.value)
        assert logger.get_events() == [], "rejected emit must not produce a record"

    def test_chain_intact_after_rejection(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="chain",
            allowed_events={"ingest"},
        )
        logger.log("ingest", {"i": 1})
        with pytest.raises(UnknownAuditEvent):
            logger.log("typoed_event", {})
        logger.log("ingest", {"i": 2})
        ok, errors = logger.verify_chain()
        assert ok is True, errors
        assert len(logger.get_events()) == 2

    def test_seq_not_incremented_on_rejection(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="seq",
            allowed_events={"ingest"},
        )
        logger.log("ingest", {})
        before = logger._seq
        with pytest.raises(UnknownAuditEvent):
            logger.log("nope", {})
        assert logger._seq == before, (
            "seq must not advance when an emit is rejected pre-write"
        )


class TestAllowlistTypes:
    def test_accepts_list(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="list",
            allowed_events=["ingest", "transform"],
        )
        logger.log("ingest", {})
        with pytest.raises(UnknownAuditEvent):
            logger.log("classify", {})

    def test_accepts_tuple(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="tup",
            allowed_events=("ingest", "transform"),
        )
        logger.log("transform", {})
        with pytest.raises(UnknownAuditEvent):
            logger.log("nope", {})

    def test_empty_allowlist_blocks_everything(self, tmp_path):
        logger = AuditLogger(
            log_dir=str(tmp_path),
            project_id="empty",
            allowed_events=[],
        )
        with pytest.raises(UnknownAuditEvent):
            logger.log("ingest", {})
