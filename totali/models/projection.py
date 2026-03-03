"""Spatial-to-language embedding projector for early fusion."""

from __future__ import annotations

import torch
from torch import nn


class TotaliMultimodalProjector(nn.Module):
    """Project point-cloud tensors into fixed-count LLM-aligned spatial tokens."""

    def __init__(
        self,
        input_dim: int = 3,
        hidden_dim: int = 1024,
        output_dim: int = 4096,
        num_spatial_tokens: int = 64,
    ) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.num_spatial_tokens = num_spatial_tokens

        self.pre_norm = nn.LayerNorm(input_dim)
        self.mlp = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, output_dim),
        )
        # Adaptive pooling yields deterministic token count regardless of point density.
        self.token_pool = nn.AdaptiveAvgPool1d(num_spatial_tokens)
        self.post_norm = nn.LayerNorm(output_dim)

    def forward(self, point_cloud: torch.Tensor) -> torch.Tensor:
        """
        Args:
            point_cloud: Shape (B, N, 3) or (N, 3).
        Returns:
            Tensor with shape (B, 64, 4096) by default.
        """
        if point_cloud.dim() == 2:
            point_cloud = point_cloud.unsqueeze(0)
        if point_cloud.dim() != 3:
            raise ValueError(f"Expected point cloud rank 3, got shape {tuple(point_cloud.shape)}")
        if point_cloud.size(-1) != self.input_dim:
            raise ValueError(
                f"Expected point cloud feature dim {self.input_dim}, got {point_cloud.size(-1)}"
            )

        x = self.pre_norm(point_cloud)
        x = self.mlp(x)  # (B, N, output_dim)
        x = x.transpose(1, 2)  # (B, output_dim, N)
        x = self.token_pool(x)  # (B, output_dim, num_spatial_tokens)
        x = x.transpose(1, 2).contiguous()  # (B, num_spatial_tokens, output_dim)
        return self.post_norm(x)
