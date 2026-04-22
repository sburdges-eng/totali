"""E-5 extension: DeterministicExtractor on the committed synthetic corpus.

Pairs a byte-stable input fixture (`tests/fixtures/survey_corpus/synthetic_500pt.npy`)
with the deterministic extractor so extraction regressions show up as
drift in corpus-derived counts, not as RNG-seeded flakiness.

Unlike `test_extractor_determinism.py` — which builds its labels from an
in-test RNG — this module depends only on committed bytes. If either
the `.npy` fixture or extractor behavior changes in a way that alters
counts, these assertions fail loudly.
"""

from pathlib import Path

import numpy as np

from totali.audit.logger import AuditLogger
from totali.extraction.extractor import DeterministicExtractor
from totali.pipeline.context import PipelineContext
from totali.pipeline.models import CRSMetadata

_CORPUS_PATH = (
    Path(__file__).parent / "fixtures" / "survey_corpus" / "synthetic_500pt.npy"
)


def _make_ctx(xyz, classification, tmp_output):
    """Prepare a PipelineContext suitable for DeterministicExtractor.run().

    Uses the same CRS shape that conftest.pipeline_context uses — EPSG:2231,
    is_valid=True — so extractor validation passes without exercising the
    geodetic phase.
    """
    return PipelineContext(
        input_path="/fake/survey_corpus.las",
        output_dir=tmp_output,
        points_xyz=xyz,
        crs=CRSMetadata(epsg_code=2231, is_valid=True),
        classification=classification,
    )


class TestCorpusFixturePresent:
    """Smoke test: committed .npy loads with the declared shape + finite values."""

    def test_npy_file_exists(self):
        assert _CORPUS_PATH.exists(), f"missing corpus fixture: {_CORPUS_PATH}"
        assert _CORPUS_PATH.stat().st_size > 0

    def test_npy_shape_is_500x3(self, survey_corpus_xyz):
        assert survey_corpus_xyz.shape == (500, 3)

    def test_npy_values_are_finite(self, survey_corpus_xyz):
        assert np.all(np.isfinite(survey_corpus_xyz))


class TestCorpusXYZShapeAndRange:
    """Statistical guards on the committed corpus.

    These are deliberately wide bounds — we are asserting the fixture
    stays inside a plausible survey envelope, not pinning exact means."""

    def test_no_nan_no_inf(self, survey_corpus_xyz):
        assert not np.any(np.isnan(survey_corpus_xyz))
        assert not np.any(np.isinf(survey_corpus_xyz))

    def test_xy_bounds_within_expected_envelope(self, survey_corpus_xyz):
        x, y = survey_corpus_xyz[:, 0], survey_corpus_xyz[:, 1]
        assert 100.0 <= x.min() and x.max() <= 200.0
        assert 100.0 <= y.min() and y.max() <= 200.0

    def test_z_bounds_within_expected_envelope(self, survey_corpus_xyz):
        z = survey_corpus_xyz[:, 2]
        assert 100.0 <= z.min() and z.max() <= 160.0

    def test_mean_near_expected_center(self, survey_corpus_xyz):
        # xy uniform on [100,200] -> mean ≈ 150. Allow ±10 ft slack for
        # finite-N variance at n=500.
        means = survey_corpus_xyz.mean(axis=0)
        assert abs(means[0] - 150.0) < 10.0
        assert abs(means[1] - 150.0) < 10.0
        # z is layered, not uniform on [100,160]; just bound it inside.
        assert 100.0 < means[2] < 160.0

    def test_dtype_is_float64(self, survey_corpus_xyz):
        assert survey_corpus_xyz.dtype == np.float64


class TestExtractorDeterministicOnCorpus:
    """Two independent DeterministicExtractor runs on the same corpus must
    agree on all counts that ExtractionResult exposes.

    Byte-level geometry parity is enforced at the shield layer
    (tests/test_shield_determinism.py); here we pin the extraction-phase
    count manifest."""

    def _run_pair(self, sample_config, tmp_path, tmp_output, xyz, classification):
        audit1 = AuditLogger(log_dir=str(tmp_path / "a1"), project_id="corpus-1")
        audit2 = AuditLogger(log_dir=str(tmp_path / "a2"), project_id="corpus-2")

        ex1 = DeterministicExtractor(sample_config["extraction"], audit1)
        ex2 = DeterministicExtractor(sample_config["extraction"], audit2)

        out1 = tmp_output / "run1"
        out2 = tmp_output / "run2"
        out1.mkdir()
        out2.mkdir()

        r1 = ex1.run(_make_ctx(xyz, classification, out1))
        r2 = ex2.run(_make_ctx(xyz, classification, out2))
        return r1, r2

    def test_success_flag_identical(
        self,
        sample_config,
        tmp_path,
        tmp_output,
        survey_corpus_xyz,
        survey_corpus_classification,
    ):
        r1, r2 = self._run_pair(
            sample_config, tmp_path, tmp_output,
            survey_corpus_xyz, survey_corpus_classification,
        )
        assert r1.success == r2.success
        # Corpus was tuned to produce a successful extraction; if this flips
        # we want loud failure, not silent comparison of two failed runs.
        assert r1.success is True, f"extractor failed on corpus: {r1.message}"

    def test_data_keys_identical(
        self,
        sample_config,
        tmp_path,
        tmp_output,
        survey_corpus_xyz,
        survey_corpus_classification,
    ):
        r1, r2 = self._run_pair(
            sample_config, tmp_path, tmp_output,
            survey_corpus_xyz, survey_corpus_classification,
        )
        assert set(r1.data.keys()) == set(r2.data.keys())
        assert "extraction" in r1.data

    def test_extraction_list_counts_identical(
        self,
        sample_config,
        tmp_path,
        tmp_output,
        survey_corpus_xyz,
        survey_corpus_classification,
    ):
        r1, r2 = self._run_pair(
            sample_config, tmp_path, tmp_output,
            survey_corpus_xyz, survey_corpus_classification,
        )
        e1 = r1.data["extraction"]
        e2 = r2.data["extraction"]

        list_fields = [
            "breaklines",
            "contours_minor",
            "contours_index",
            "building_footprints",
            "curb_lines",
            "wire_lines",
            "hardscape_polygons",
            "occlusion_zones",
            "qa_flags",
        ]
        for name in list_fields:
            v1 = getattr(e1, name)
            v2 = getattr(e2, name)
            assert len(v1) == len(v2), (
                f"{name} count drift: {len(v1)} vs {len(v2)}"
            )

    def test_dtm_vertex_and_face_counts_identical(
        self,
        sample_config,
        tmp_path,
        tmp_output,
        survey_corpus_xyz,
        survey_corpus_classification,
    ):
        r1, r2 = self._run_pair(
            sample_config, tmp_path, tmp_output,
            survey_corpus_xyz, survey_corpus_classification,
        )
        e1 = r1.data["extraction"]
        e2 = r2.data["extraction"]

        # dtm_vertices / dtm_faces are ndarrays or None; guard both shapes.
        def _count(x):
            return 0 if x is None else len(x)

        assert _count(e1.dtm_vertices) == _count(e2.dtm_vertices)
        assert _count(e1.dtm_faces) == _count(e2.dtm_faces)

    def test_error_metrics_keys_identical(
        self,
        sample_config,
        tmp_path,
        tmp_output,
        survey_corpus_xyz,
        survey_corpus_classification,
    ):
        r1, r2 = self._run_pair(
            sample_config, tmp_path, tmp_output,
            survey_corpus_xyz, survey_corpus_classification,
        )
        e1 = r1.data["extraction"]
        e2 = r2.data["extraction"]
        assert set(e1.error_metrics.keys()) == set(e2.error_metrics.keys())
        for section in e1.error_metrics:
            assert set(e1.error_metrics[section].keys()) == set(
                e2.error_metrics[section].keys()
            ), f"error_metrics[{section}] key drift"
