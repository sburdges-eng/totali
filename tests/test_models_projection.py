"""M-3 partial: TotaliMultimodalProjector pure-math coverage.

Tests for `totali.models.projection.TotaliMultimodalProjector` — shape
guarantees, determinism under a seeded weight init, and input validation.
Heavy torch, but deterministic on CPU and cheap enough for the unit suite.
"""

import pytest

torch = pytest.importorskip("torch")

from totali.models.projection import TotaliMultimodalProjector  # noqa: E402


@pytest.fixture
def projector():
    torch.manual_seed(42)
    return TotaliMultimodalProjector(
        input_dim=3, hidden_dim=32, output_dim=64, num_spatial_tokens=8
    ).eval()


class TestShape:
    def test_batch_input_shape(self, projector):
        with torch.no_grad():
            pc = torch.randn(2, 100, 3)
            out = projector(pc)
        assert out.shape == (2, 8, 64)

    def test_unbatched_input_is_promoted(self, projector):
        with torch.no_grad():
            pc = torch.randn(100, 3)
            out = projector(pc)
        assert out.shape == (1, 8, 64)

    def test_token_count_independent_of_point_count(self, projector):
        with torch.no_grad():
            small = projector(torch.randn(1, 16, 3))
            large = projector(torch.randn(1, 10000, 3))
        assert small.shape[1] == large.shape[1] == 8


class TestInputValidation:
    def test_wrong_feature_dim_raises(self, projector):
        with torch.no_grad():
            with pytest.raises(ValueError) as exc:
                projector(torch.randn(1, 100, 5))
        assert "feature dim" in str(exc.value).lower() or "expected" in str(exc.value).lower()

    def test_rank_1_raises(self, projector):
        with torch.no_grad():
            with pytest.raises(ValueError):
                projector(torch.randn(100))


class TestDeterminism:
    def test_same_seed_same_output(self):
        torch.manual_seed(0)
        a = TotaliMultimodalProjector(3, 16, 32, 4).eval()
        torch.manual_seed(0)
        b = TotaliMultimodalProjector(3, 16, 32, 4).eval()
        with torch.no_grad():
            pc = torch.randn(1, 50, 3, generator=torch.Generator().manual_seed(1))
            out_a = a(pc)
            out_b = b(pc)
        assert torch.allclose(out_a, out_b)

    def test_repeated_forward_same_output(self, projector):
        with torch.no_grad():
            pc = torch.randn(1, 50, 3)
            out1 = projector(pc)
            out2 = projector(pc)
        assert torch.allclose(out1, out2)
