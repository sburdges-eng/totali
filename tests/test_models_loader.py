"""M-1/M-2: model loader — file-existence + SHA-256 manifest validation."""

import hashlib
import json

import pytest

from totali.models.loader import (
    ManifestError,
    ModelHashMismatch,
    ModelNotFoundError,
    compute_sha256,
    load_manifest,
    load_onnx,
)


class TestComputeSha256:
    def test_matches_manual(self, tmp_path):
        f = tmp_path / "x.bin"
        f.write_bytes(b"hello totali")
        expected = hashlib.sha256(b"hello totali").hexdigest()
        assert compute_sha256(f) == expected

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        assert compute_sha256(f) == hashlib.sha256(b"").hexdigest()


class TestLoadManifest:
    def test_missing_manifest_returns_empty(self, tmp_path):
        assert load_manifest(tmp_path / "nope.json") == {}

    def test_well_formed(self, tmp_path):
        path = tmp_path / "MANIFEST.json"
        path.write_text(json.dumps({
            "seg.onnx": {"filename": "seg.onnx", "sha256": "a" * 64}
        }))
        m = load_manifest(path)
        assert "seg.onnx" in m
        assert m["seg.onnx"]["sha256"] == "a" * 64

    def test_bad_json_raises(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json")
        with pytest.raises(ManifestError):
            load_manifest(path)

    def test_non_object_root_raises(self, tmp_path):
        path = tmp_path / "arr.json"
        path.write_text(json.dumps(["seg.onnx"]))
        with pytest.raises(ManifestError):
            load_manifest(path)

    def test_missing_required_field_raises(self, tmp_path):
        path = tmp_path / "m.json"
        path.write_text(json.dumps({"seg.onnx": {"filename": "seg.onnx"}}))
        with pytest.raises(ManifestError) as exc:
            load_manifest(path)
        assert "sha256" in str(exc.value)


class TestLoadOnnx:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ModelNotFoundError):
            load_onnx(tmp_path / "ghost.onnx")

    def test_expected_sha256_mismatch_raises(self, tmp_path):
        m = tmp_path / "m.onnx"
        m.write_bytes(b"fake-onnx-bytes")
        with pytest.raises(ModelHashMismatch):
            load_onnx(m, expected_sha256="0" * 64)

    def test_manifest_sha256_mismatch_raises(self, tmp_path):
        m = tmp_path / "m.onnx"
        m.write_bytes(b"fake-onnx-bytes")
        manifest = {"m.onnx": {"filename": "m.onnx", "sha256": "0" * 64}}
        with pytest.raises(ModelHashMismatch):
            load_onnx(m, manifest=manifest)

    def test_matching_hash_proceeds_to_onnx_layer(self, tmp_path):
        """Hash match must clear the guard; onnxruntime stub then raises ImportError.

        The fake InferenceSession in conftest raises ImportError on
        construction, so a successful hash path propagates that exception —
        proving we reached the onnxruntime layer.
        """
        m = tmp_path / "m.onnx"
        data = b"fake-onnx-bytes"
        m.write_bytes(data)
        actual = hashlib.sha256(data).hexdigest()
        with pytest.raises(ImportError):
            load_onnx(m, expected_sha256=actual)
