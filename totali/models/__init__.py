"""Model component exports.

`TotaliMultimodalProjector` requires `torch`. Imports guarded so that
environments without torch (e.g. the minimal test venv) can still load
`totali.models.loader` and other sibling modules.
"""

try:
    from .projection import TotaliMultimodalProjector  # noqa: F401

    __all__ = ["TotaliMultimodalProjector"]
except ImportError:  # pragma: no cover - environment-dependent
    __all__ = []
