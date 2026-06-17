"""U1 foundation: Carlson field-code (``.fld``) taxonomy loader.

Loads a surveyor field-code library at **runtime** from a configurable path (the
gitignored partner-data dir) — never hardcoding client codes into source. It
yields two things the rest of the pipeline needs:

* the **classifier class set** — the distinct layers a coded point can resolve to
  (used by segmentation / U1), and
* the deterministic **code -> layer / symbol / linework** map that downstream
  drafting (U4 / ``cad_shielding``) reuses to place geometry on the right layer.

Format notes (Carlson 2010V ``.fld``, pipe-delimited):

* The first line is a header beginning with ``#`` (e.g. ``#2010V# Code|...``).
* A template row whose code column is ``FIELD CODE`` is a placeholder, not a code.
* Despite the header labelling column 1 "Description", the **value** in column 1
  is the destination **layer** (confirmed against the firm's descriptions CSV),
  column 2 is the symbol.
* Linework (YES/NO) is taken from the companion ``FGD_Field_Code_Descriptions``
  CSV when supplied; without it, linework is left unknown (``None``).

The library files are confidential partner artifacts (see ``totali/data/``, which
is gitignored). This module only *reads* them; tests use synthetic fixtures, so no
client data is committed.
"""

from __future__ import annotations

import csv as _csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

#: Env vars that point at the field-code library; resolved lazily, never at import.
FLD_PATH_ENV = "TOTALI_FIELDCODE_FLD"
DESCRIPTIONS_PATH_ENV = "TOTALI_FIELDCODE_DESCRIPTIONS"

#: Placeholder row in a ``.fld`` whose code column is this is not a real code.
_TEMPLATE_CODE = "FIELD CODE"

#: Extracts layer / symbol / linework from a descriptions-CSV sentence, e.g.
#: "Field code 'CP' is placed on layer 'CONTROL_POINT', uses symbol 'CTRLPT',
#:  and linework is set to 'NO'."
_DESC_RE = re.compile(
    r"placed on layer '(?P<layer>[^']*)'.*?"
    r"uses symbol '(?P<symbol>[^']*)'.*?"
    r"linework is set to '(?P<linework>[^']*)'",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class FieldCode:
    """One surveyor field code and where its points/linework belong."""

    code: str
    layer: str
    symbol: str
    linework: Optional[bool]  # None = unknown (no descriptions source supplied)

    @property
    def is_point_feature(self) -> bool:
        """True only when linework is known-False (a discrete point/marker)."""
        return self.linework is False


class FieldCodeTable:
    """Immutable lookup over a loaded field-code library."""

    def __init__(self, codes: dict[str, FieldCode]):
        self._codes: dict[str, FieldCode] = dict(codes)

    def __len__(self) -> int:
        return len(self._codes)

    def __contains__(self, code: object) -> bool:
        return code in self._codes

    def __getitem__(self, code: str) -> FieldCode:
        return self._codes[code]

    def get(self, code: str) -> Optional[FieldCode]:
        return self._codes.get(code)

    def codes(self) -> tuple[str, ...]:
        return tuple(self._codes)

    def classes(self) -> tuple[str, ...]:
        """Classifier class set: the distinct destination layers, sorted/stable."""
        return tuple(sorted({fc.layer for fc in self._codes.values()}))

    def layer_for(self, code: str, default: Optional[str] = None) -> Optional[str]:
        fc = self._codes.get(code)
        return fc.layer if fc else default

    def symbol_for(self, code: str, default: Optional[str] = None) -> Optional[str]:
        fc = self._codes.get(code)
        return fc.symbol if fc else default

    def is_linework(self, code: str) -> Optional[bool]:
        fc = self._codes.get(code)
        return fc.linework if fc else None

    def linework_codes(self) -> tuple[str, ...]:
        return tuple(c for c, fc in self._codes.items() if fc.linework is True)

    def point_codes(self) -> tuple[str, ...]:
        return tuple(c for c, fc in self._codes.items() if fc.linework is False)


def _parse_descriptions(path: Path) -> dict[str, bool]:
    """Map field code -> linework(bool) from the FGD descriptions CSV."""
    out: dict[str, bool] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            code = (row.get("Field Code") or "").strip()
            if not code:
                continue
            match = _DESC_RE.search(row.get("Description") or "")
            if match:
                out[code] = match.group("linework").strip().upper() == "YES"
    return out


def _first_data_line(path: Path) -> str:
    with path.open(encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if line:
                return line
    return ""


def _is_csv_format(path: Path) -> bool:
    """CSV-style library (``Field Code,Layer,Symbol,Linework``) vs pipe 2010V."""
    first = _first_data_line(path)
    return "|" not in first and "," in first


def _parse_pipe_fld(path: Path, linework_by_code: dict[str, bool]) -> dict[str, FieldCode]:
    """Carlson 2010V pipe format; layer=col1, symbol=col2; linework from CSV."""
    codes: dict[str, FieldCode] = {}
    with path.open(encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):  # blank or header line
                continue
            fields = line.split("|")
            if len(fields) < 3:
                continue
            code = fields[0].strip()
            if not code or code == _TEMPLATE_CODE:
                continue
            codes[code] = FieldCode(
                code=code,
                layer=fields[1].strip(),
                symbol=fields[2].strip(),
                linework=linework_by_code.get(code),
            )
    return codes


def _parse_csv_fld(path: Path) -> dict[str, FieldCode]:
    """Simple CSV library: ``Field Code,Layer,Symbol,Linework`` with inline linework."""
    codes: dict[str, FieldCode] = {}
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        for row in _csv.reader(f):
            if not row:
                continue
            code = row[0].strip()
            if not code or code in ("Field Code", _TEMPLATE_CODE):
                continue
            layer = row[1].strip() if len(row) > 1 else ""
            symbol = row[2].strip() if len(row) > 2 else ""
            linework: Optional[bool] = None
            if len(row) > 3 and row[3].strip():
                linework = row[3].strip().upper() == "YES"
            codes[code] = FieldCode(code=code, layer=layer, symbol=symbol, linework=linework)
    return codes


def load_field_codes(
    fld_path: os.PathLike[str] | str,
    descriptions_path: os.PathLike[str] | str | None = None,
) -> FieldCodeTable:
    """Parse a field-code library into a table, auto-detecting the format.

    Supports the Carlson 2010V pipe ``.fld`` (layer=col1, symbol=col2; linework
    from a companion descriptions CSV) and the simple CSV library
    (``Field Code,Layer,Symbol,Linework`` with inline linework).

    Raises :class:`FileNotFoundError` if ``fld_path`` is missing. A missing or
    unreadable descriptions CSV is tolerated (pipe-format linework left unknown).
    """
    fld = Path(fld_path)
    if not fld.exists():
        raise FileNotFoundError(f"field-code library not found: {fld}")

    if _is_csv_format(fld):
        return FieldCodeTable(_parse_csv_fld(fld))

    linework_by_code: dict[str, bool] = {}
    if descriptions_path is not None:
        desc = Path(descriptions_path)
        if desc.exists():
            linework_by_code = _parse_descriptions(desc)
    return FieldCodeTable(_parse_pipe_fld(fld, linework_by_code))


def load_default() -> FieldCodeTable:
    """Load the field-code table from the configured env paths.

    Reads :data:`FLD_PATH_ENV` (required) and :data:`DESCRIPTIONS_PATH_ENV`
    (optional). Kept out of import time so absence never breaks module import.
    """
    fld = os.environ.get(FLD_PATH_ENV)
    if not fld:
        raise RuntimeError(
            f"set {FLD_PATH_ENV} to the field-code .fld path "
            "(confidential partner data lives under the gitignored totali/data/)"
        )
    return load_field_codes(fld, os.environ.get(DESCRIPTIONS_PATH_ENV))
