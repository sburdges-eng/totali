"""L-4: auto_promote cannot be turned on through config.

Core TOTaLi invariant — AI/ML output lands on DRAFT layers only. Promotion to
certified status is a human decision. This guard asserts that no configuration
value can override the hardcoded `auto_promote = False`.
"""

import pytest

from totali.linting.surveyor_lint import SurveyorLinter


@pytest.fixture
def _mk(audit_logger):
    def _build(config_overrides):
        base = {
            "ghost_opacity": 0.4,
            "flag_colors": {},
            "auto_promote": False,
            "require_pls_signature": True,
        }
        base.update(config_overrides)
        return SurveyorLinter(base, audit_logger)

    return _build


class TestAutoPromoteGuard:
    def test_default_false(self, _mk):
        linter = _mk({})
        assert linter.auto_promote is False

    @pytest.mark.parametrize("truthy", [True, "true", "yes", 1, "1"])
    def test_config_truthy_rejected(self, _mk, truthy):
        """L-4 hard rejection: truthy auto_promote raises AutoPromoteForbidden."""
        from totali.linting.surveyor_lint import AutoPromoteForbidden

        with pytest.raises(AutoPromoteForbidden) as exc:
            _mk({"auto_promote": truthy})
        assert "auto_promote" in str(exc.value)

    def test_require_pls_default_true(self, _mk):
        linter = _mk({})
        assert linter.require_pls is True

    def test_require_pls_honored_when_false(self, _mk):
        # Note: require_pls_signature IS honored from config (operator choice);
        # only auto_promote is hardcoded. This test documents that distinction.
        linter = _mk({"require_pls_signature": False})
        assert linter.require_pls is False
        assert linter.auto_promote is False
