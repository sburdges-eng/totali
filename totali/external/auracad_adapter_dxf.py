"""Local DXF-backed ``AuracadAdapter`` implementation (Phase 1).

This is the first live implementation of the ``AuracadAdapter`` Protocol
defined in :mod:`totali.external.auracad_contract`. It closes the
contract loop for ``read_cad`` by delegating to the in-tree ezdxf-backed
DXF parser (``dwg-tool-parser/scripts/parse_dxf.py``) — no auracad FFI is
required to unblock TOTaLi's ingest path on pure-DXF inputs.

The heavier-weight methods (``heal_geometry``, ``transform_crs``) are
stubs that raise :class:`NotImplementedError` with guidance. They'll be
replaced once the real auracad bridge (FFI or subprocess) lands; until
then, callers should either wire those paths through auracad directly or
fall back to TOTaLi's own CADShield / ``pyproj`` helpers.

This adapter is intentionally **not** exported from
:mod:`totali.external.__init__` — that module is the frozen
contract-surface index. Consumers import this concrete class explicitly.
"""

from __future__ import annotations

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from totali.external.auracad_contract import (
    AuracadHealReport,
    AuracadReadReport,
    AuracadTransformReport,
)

# ---------------------------------------------------------------------------
# parse_dxf loader
# ---------------------------------------------------------------------------
#
# ``dwg-tool-parser/scripts/parse_dxf.py`` lives under a hyphenated directory
# so ``import dwg_tool_parser.scripts.parse_dxf`` is not available without
# path gymnastics. The existing test suite (see
# ``tests/test_dwg_parser_ezdxf.py``) loads the module by file spec — we
# replicate that pattern here, module-scoped so we only pay the load cost
# once per process.

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PARSE_DXF_PATH = _REPO_ROOT / "dwg-tool-parser" / "scripts" / "parse_dxf.py"

# Map from the parser's JSON camelCase keys to the Pydantic snake_case
# field names on :class:`AuracadReadReport`. All other keys already match.
_PARSER_FIELD_RENAMES = {"schemaVersion": "schema_version"}

_parse_dxf_module: ModuleType | None = None


def _load_parse_dxf_module() -> ModuleType:
    """Load ``parse_dxf.py`` by file spec and cache the module."""

    global _parse_dxf_module
    if _parse_dxf_module is not None:
        return _parse_dxf_module

    spec = spec_from_file_location("parse_dxf_script", _PARSE_DXF_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(
            f"could not build import spec for {_PARSE_DXF_PATH}"
        )
    module = module_from_spec(spec)
    # Register in sys.modules BEFORE exec_module so re-entrant loads, reload
    # scenarios, and circular refs within parse_dxf resolve to a single
    # canonical module object rather than silently duplicating state.
    sys.modules["parse_dxf_script"] = module
    spec.loader.exec_module(module)
    _parse_dxf_module = module
    return module


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class DxfAuracadAdapter:
    """Local DXF-backed AuracadAdapter implementation.

    Satisfies AuracadAdapter.read_cad by delegating to dwg-tool-parser.
    heal_geometry and transform_crs are stubs pending real auracad FFI.
    """

    def read_cad(self, path: str) -> AuracadReadReport:
        """Parse a DXF file and return a contract-shaped report.

        Delegates to the in-tree ezdxf-backed parser. Any exception raised
        by ``parse_dxf`` (missing file, corrupt DXF, ezdxf internal error)
        propagates unchanged — the adapter does not swallow failures.
        """

        module = _load_parse_dxf_module()
        raw = module.parse_dxf(path)

        # Translate camelCase schema keys to the Pydantic field names.
        payload = {
            _PARSER_FIELD_RENAMES.get(key, key): value
            for key, value in raw.items()
        }
        return AuracadReadReport(**payload)

    def heal_geometry(
        self,
        polylines: list[list[tuple[float, float, float]]],
        tolerance: float,
    ) -> AuracadHealReport:
        raise NotImplementedError(
            "DxfAuracadAdapter.heal_geometry is not implemented. "
            "Wire the real auracad FFI or use TOTaLi's CADShield healer."
        )

    def transform_crs(
        self,
        xyz: list[tuple[float, float, float]],
        source_epsg: int,
        target_epsg: int,
    ) -> AuracadTransformReport:
        raise NotImplementedError(
            "DxfAuracadAdapter.transform_crs is not implemented. "
            "Use pyproj.Transformer directly or wire the real auracad FFI."
        )
