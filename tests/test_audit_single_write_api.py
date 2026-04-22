"""A-8: AuditLogger exposes exactly one write surface (`log`).

Defensive guard against drift. Future refactors that add an alternative
write path (e.g. `log_raw`, `append_record`, `bulk_log`) trip this test
and force an explicit decision: either re-bless the new surface (update
this test) or remove it (preserve the single-writer invariant).

The single-writer invariant matters because every write must traverse the
hash-chain logic in `log()`. A bypass surface — even one that looks
innocuous — could allow records to land without `prev_hash` linkage.
"""

import inspect

from totali.audit.logger import AuditLogger


# Methods that legitimately appear on AuditLogger but are NOT write paths.
_NON_WRITE_METHODS = frozenset({
    "__init__",
    "verify_chain",
    "get_events",
    "summary",
    "close",  # close() calls log() internally; not an alt write path
})

# The ONE blessed write API.
_WRITE_METHODS = frozenset({"log"})


def _public_methods(cls):
    return frozenset(
        name for name, attr in inspect.getmembers(cls, inspect.isfunction)
        if not name.startswith("_")
    )


class TestSingleWriteSurface:
    def test_log_is_the_only_writer(self):
        """Any new public method must be classified — either bless it as a
        write surface (update _WRITE_METHODS) or as non-write (update
        _NON_WRITE_METHODS). Drift fails loudly."""
        public = _public_methods(AuditLogger)
        unclassified = public - _WRITE_METHODS - _NON_WRITE_METHODS
        assert not unclassified, (
            f"AuditLogger has unclassified public methods: {sorted(unclassified)}. "
            "Bless each as a write surface or non-write in this test, then "
            "verify the single-writer invariant still holds."
        )

    def test_write_methods_set_is_singleton(self):
        assert _WRITE_METHODS == {"log"}, (
            "TOTaLi A-8 invariant: AuditLogger.log is the only blessed write "
            "surface. Adding another writer requires explicit re-blessing here "
            "AND a design review of the hash-chain bypass risk."
        )

    def test_log_signature_unchanged(self):
        """Argument names + count are part of the contract; downstream callers
        and the agent orchestration spec depend on them."""
        sig = inspect.signature(AuditLogger.log)
        params = list(sig.parameters.keys())
        # self + event_type + data
        assert params == ["self", "event_type", "data"], params


class TestNoBypassPaths:
    def test_no_open_method(self):
        """No public method named anything like open/append/write/bulk."""
        public = _public_methods(AuditLogger)
        suspicious_substrings = ("write", "append", "bulk", "raw", "direct")
        suspicious = {
            m for m in public
            if any(s in m.lower() for s in suspicious_substrings)
        }
        assert not suspicious, (
            f"AuditLogger has suspicious-looking public methods: {sorted(suspicious)}. "
            "If they are legitimate, add to _NON_WRITE_METHODS in this test."
        )

    def test_log_path_attribute_is_path_only(self):
        """log_path exposed for inspection (verify_chain, tests) but is not "
        a write surface."""
        from pathlib import Path
        logger = AuditLogger(log_dir="/tmp/totali_test_a8", project_id="a8")
        assert isinstance(logger.log_path, Path)
