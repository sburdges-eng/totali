"""M-1/M-2: deterministic ONNX model loader + manifest helpers.

Loader responsibilities (see `totali/models/AGENTIC.md`):

- File existence and readability
- Optional SHA-256 hash check against a `models/MANIFEST.json` entry or a
  caller-supplied `expected_sha256`
- Deterministic session config flags available to callers
- Refusal to download; operators provision out-of-band

The loader is intentionally thin — heavy lifting happens inside
`onnxruntime`, which may not be installed in minimal environments. The
public surface is:

    load_onnx(path, *, device="cpu", deterministic=True,
              expected_sha256=None, manifest=None)
        -> onnxruntime.InferenceSession

    compute_sha256(path) -> str
    load_manifest(manifest_path=None) -> dict
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional


class ModelNotFoundError(FileNotFoundError):
    """Raised when the requested ONNX model file is absent."""


class ModelHashMismatch(ValueError):
    """Raised when the on-disk SHA-256 does not match the manifest / caller expectation."""


class ManifestError(ValueError):
    """Raised when the manifest file is malformed or references unknown fields."""


_MANIFEST_REQUIRED_FIELDS: tuple[str, ...] = ("filename", "sha256")


def compute_sha256(path: Path, chunk_size: int = 65536) -> str:
    """Streaming SHA-256 of a file. Deterministic, bounded memory."""
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(manifest_path: Optional[Path] = None) -> dict[str, dict[str, Any]]:
    """Load the models manifest (``models/MANIFEST.json`` by default).

    Returns a dict keyed by filename. Missing manifest -> empty dict (no hash
    enforcement). A malformed manifest raises :class:`ManifestError`.
    """
    path = Path(manifest_path) if manifest_path is not None else (Path("models") / "MANIFEST.json")
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text("utf-8"))
    except json.JSONDecodeError as e:
        raise ManifestError(f"manifest is not valid JSON: {e}") from e

    if not isinstance(raw, dict):
        raise ManifestError("manifest root must be a JSON object")
    normalized: dict[str, dict[str, Any]] = {}
    for name, entry in raw.items():
        if not isinstance(entry, dict):
            raise ManifestError(f"manifest[{name}] must be an object")
        missing = [f for f in _MANIFEST_REQUIRED_FIELDS if f not in entry]
        if missing:
            raise ManifestError(
                f"manifest[{name}] missing required fields: {missing}"
            )
        normalized[name] = entry
    return normalized


def load_onnx(
    path: str | Path,
    *,
    device: str = "cpu",
    deterministic: bool = True,
    expected_sha256: Optional[str] = None,
    manifest: Optional[dict[str, dict[str, Any]]] = None,
):
    """Load an ONNX model with optional hash validation.

    Returns an :class:`onnxruntime.InferenceSession`. Raises:

    - :class:`ModelNotFoundError` if the file is absent.
    - :class:`ModelHashMismatch` if SHA-256 disagrees with ``expected_sha256``
      or the manifest entry for this filename.
    - :class:`ImportError` when ``onnxruntime`` is unavailable or incomplete
      (callers should catch and fall back).
    """
    model_path = Path(path)
    if not model_path.exists():
        raise ModelNotFoundError(f"ONNX model not found: {model_path}")

    actual_hash = compute_sha256(model_path)

    if expected_sha256 is not None and actual_hash != expected_sha256:
        raise ModelHashMismatch(
            f"{model_path.name}: sha256 {actual_hash[:16]}... "
            f"does not match expected {expected_sha256[:16]}..."
        )

    if manifest is None:
        manifest = load_manifest()
    entry = manifest.get(model_path.name)
    if entry is not None and entry.get("sha256") and entry["sha256"] != actual_hash:
        raise ModelHashMismatch(
            f"{model_path.name}: sha256 {actual_hash[:16]}... "
            f"does not match manifest {entry['sha256'][:16]}..."
        )

    import onnxruntime as ort

    # Tolerate minimal stubs in test environments by treating a missing
    # InferenceSession as "runtime unavailable" and raising ImportError so
    # callers can fall back gracefully (matches the classifier's contract).
    if not hasattr(ort, "InferenceSession"):
        raise ImportError(
            "onnxruntime is installed but does not expose InferenceSession; "
            "a real onnxruntime build is required to load ONNX models."
        )

    providers = (
        ["CUDAExecutionProvider", "CPUExecutionProvider"]
        if device == "cuda"
        else ["CPUExecutionProvider"]
    )
    session_options = ort.SessionOptions() if hasattr(ort, "SessionOptions") else None
    if deterministic and session_options is not None:
        # Minimise sources of nondeterminism in the runtime.
        session_options.enable_mem_pattern = False
        session_options.intra_op_num_threads = 1
    if session_options is not None:
        return ort.InferenceSession(
            str(model_path), sess_options=session_options, providers=providers
        )
    return ort.InferenceSession(str(model_path), providers=providers)
