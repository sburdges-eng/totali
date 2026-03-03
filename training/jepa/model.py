import torch
import torch.nn as nn
import torch.nn.functional as F

class JEPAEncoder(nn.Module):
    """
    Encoder for JEPA: Processes context or target patches.
    Simple CNN-based encoder for heightmaps.
    """
    def __init__(self, in_channels=2, embed_dim=128):
        super().__init__()
        self.net = nn.Sequential(
            nn.Conv2d(in_channels, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(2),  # 100 -> 50

            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(2),  # 50 -> 25

            nn.Conv2d(64, embed_dim, kernel_size=3, padding=1),
            nn.BatchNorm2d(embed_dim),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d((1, 1)) # Global Average Pooling -> (B, embed_dim, 1, 1)
        )
        self.embed_dim = embed_dim

    def forward(self, x):
        # x: (B, C, H, W)
        x = self.net(x)
        return x.flatten(1) # (B, embed_dim)

class JEPAPredictor(nn.Module):
    """
    Predictor for JEPA: Predicts target embedding from context embedding + conditioning.
    """
    def __init__(self, embed_dim=128, hidden_dim=256):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(embed_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, embed_dim)
        )

    def forward(self, x):
        return self.net(x)

class JEPAModel(nn.Module):
    """
    Full JEPA Model structure.
    """
    def __init__(self, in_channels=2, embed_dim=128):
        super().__init__()
        self.context_encoder = JEPAEncoder(in_channels, embed_dim)
        self.target_encoder = JEPAEncoder(in_channels, embed_dim)
        self.predictor = JEPAPredictor(embed_dim)

        # Target encoder is updated via EMA, not gradients
        for p in self.target_encoder.parameters():
            p.requires_grad = False

    def forward(self, context, target):
        """
        Forward pass for training.
        context: Input context view (B, C, H, W)
        target: Input target view (B, C, H, W)
        """
        # Encode context
        context_emb = self.context_encoder(context)

        # Predict target embedding
        pred_emb = self.predictor(context_emb)

        # Encode target (with target encoder)
        with torch.no_grad():
            target_emb = self.target_encoder(target)

        return pred_emb, target_emb

    @torch.no_grad()
    def update_target_encoder(self, momentum=0.996):
        """
        EMA update of target encoder.
        """
        for param_q, param_k in zip(self.context_encoder.parameters(), self.target_encoder.parameters()):
            param_k.data = param_k.data * momentum + param_q.data * (1. - momentum)
