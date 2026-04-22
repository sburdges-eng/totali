"""Coverage for totali.audit.verify CLI — exit codes + tamper detection."""

import json


from totali.audit.logger import AuditLogger
from totali.audit.verify import main, verify_log


class TestVerifyFunction:
    def test_clean_log_verifies(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="clean")
        for i in range(5):
            logger.log("event", {"i": i})
        ok, errors = verify_log(logger.log_path)
        assert ok is True
        assert errors == []

    def test_missing_file_fails(self, tmp_path):
        ok, errors = verify_log(tmp_path / "nope.jsonl")
        assert ok is False
        assert any("does not exist" in e for e in errors)

    def test_detects_hash_mutation(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="mut")
        logger.log("a", {"x": 1})
        logger.log("b", {"x": 2})
        lines = logger.log_path.read_text().splitlines()
        rec = json.loads(lines[0])
        rec["data"]["x"] = 999
        lines[0] = json.dumps(rec)
        logger.log_path.write_text("\n".join(lines) + "\n")

        ok, errors = verify_log(logger.log_path)
        assert ok is False
        assert errors

    def test_skips_blank_lines(self, tmp_path):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="blanks")
        logger.log("x", {})
        with logger.log_path.open("a") as f:
            f.write("\n\n")
        ok, errors = verify_log(logger.log_path)
        assert ok is True
        assert errors == []


class TestVerifyCLI:
    def test_exit_0_on_clean(self, tmp_path, capsys):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="exit0")
        logger.log("event", {})
        rc = main([str(logger.log_path)])
        assert rc == 0

    def test_exit_3_on_tamper(self, tmp_path, capsys):
        logger = AuditLogger(log_dir=str(tmp_path), project_id="exit3")
        logger.log("a", {"v": 1})
        logger.log("b", {"v": 2})
        lines = logger.log_path.read_text().splitlines()
        rec = json.loads(lines[1])
        rec["prev_hash"] = "0" * 64
        lines[1] = json.dumps(rec)
        logger.log_path.write_text("\n".join(lines) + "\n")

        rc = main([str(logger.log_path), "--quiet"])
        assert rc == 3

    def test_exit_3_on_missing(self, tmp_path):
        rc = main([str(tmp_path / "no.jsonl"), "--quiet"])
        assert rc == 3
