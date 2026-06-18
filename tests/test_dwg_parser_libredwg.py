"""DP-1: built-in LibreDWG ``dwg2dxf`` default-converter path.

The DWG parser can now convert ``.dwg`` input with no explicit
``--converter-cmd`` by resolving a vendored / installed LibreDWG ``dwg2dxf``
binary. These tests cover the resolver precedence and a real DWG -> DXF -> JSON
round trip through that default path.

The end-to-end test is skipped when no ``dwg2dxf`` binary is resolvable (e.g.
CI without the vendored LibreDWG build), so the suite stays green there.
"""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_DWG_FIXTURE = (
    _REPO
    / "AUTOMATICCAD"
    / "files"
    / "Users"
    / "seanburdges"
    / "Downloads"
    / "21-034 PRELIM 220717.dwg"
)


def _load_parser_module():
    parser_path = (
        _REPO / "survey-automation-roadmap" / "dwg-tool-parser" / "scripts" / "parse_dwg.py"
    )
    spec = spec_from_file_location("parse_dwg_script_libredwg", parser_path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def parser():
    return _load_parser_module()


class TestResolveDwg2Dxf:
    def test_env_override_takes_precedence(self, parser, tmp_path, monkeypatch):
        fake = tmp_path / "dwg2dxf"
        fake.write_text("#!/bin/sh\nexit 0\n")
        fake.chmod(0o755)
        monkeypatch.setenv("LIBREDWG_DWG2DXF", str(fake))
        assert parser.resolve_dwg2dxf() == str(fake)

    def test_env_value_ignored_when_not_executable(self, parser, tmp_path, monkeypatch):
        # A non-existent env path must fall through, never be returned verbatim.
        monkeypatch.setenv("LIBREDWG_DWG2DXF", str(tmp_path / "does-not-exist"))
        resolved = parser.resolve_dwg2dxf()
        assert resolved != str(tmp_path / "does-not-exist")


class TestLibreDwgDefaultConversion:
    def test_dwg_converted_via_libredwg(self, parser):
        dwg2dxf_bin = parser.resolve_dwg2dxf()
        if dwg2dxf_bin is None:
            pytest.skip("no dwg2dxf binary resolvable (vendored LibreDWG absent)")
        if not _DWG_FIXTURE.is_file():
            pytest.skip(f"DWG fixture missing: {_DWG_FIXTURE}")

        result, conversion_info, temp_dir = parser.parse_input(
            input_path=_DWG_FIXTURE,
            converter_template=None,  # force the built-in default path
            sample_limit=10,
            precision=6,
        )
        try:
            # Conversion went through the vendored/resolved LibreDWG default.
            assert conversion_info["used"] is True
            assert conversion_info["converter"] == "libredwg"
            assert conversion_info["dwg2dxf_bin"] == dwg2dxf_bin
            assert Path(conversion_info["output_dxf"]).is_file()
            # The converted DXF parsed into a structured summary.
            assert "summary" in result
            assert isinstance(result["summary"]["entity_total"], int)
            assert result["summary"]["entity_total"] >= 0
        finally:
            if temp_dir is not None:
                import shutil as _sh

                _sh.rmtree(temp_dir, ignore_errors=True)


class TestResolverRobustness:
    def test_skips_nonrunnable_binary(self, parser, tmp_path, monkeypatch):
        # A binary that exists and is executable but FAILS to run (e.g. a broken
        # dynamic link) must not be selected over a working one or None.
        broken = tmp_path / "dwg2dxf"
        broken.write_text("#!/bin/sh\nexit 1\n")
        broken.chmod(0o755)
        monkeypatch.setenv("LIBREDWG_DWG2DXF", str(broken))
        monkeypatch.setattr(parser.shutil, "which", lambda _: None)
        assert parser.resolve_dwg2dxf() != str(broken)
