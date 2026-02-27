import torch
import torch.nn as nn
import torch.nn.functional as F

class PointNet(nn.Module):
    """
    Simple PointNet-like architecture for point cloud segmentation.
    """
    def __init__(self, in_channels=3, num_classes=10):
        super(PointNet, self).__init__()
        self.conv1 = nn.Conv1d(in_channels, 64, 1)
        self.conv2 = nn.Conv1d(64, 128, 1)
        self.conv3 = nn.Conv1d(128, 1024, 1)

        self.fc1 = nn.Linear(1024, 512)
        self.fc2 = nn.Linear(512, 256)
        self.fc3 = nn.Linear(256, num_classes)

        self.bn1 = nn.BatchNorm1d(64)
        self.bn2 = nn.BatchNorm1d(128)
        self.bn3 = nn.BatchNorm1d(1024)
        self.bn4 = nn.BatchNorm1d(512)
        self.bn5 = nn.BatchNorm1d(256)

    def forward(self, x):
        # x: (B, N, C) -> (B, C, N)
        x = x.transpose(2, 1)
        B, C, N = x.size()

        x = F.relu(self.bn1(self.conv1(x)))
        x = F.relu(self.bn2(self.conv2(x)))
        x = F.relu(self.bn3(self.conv3(x)))

        # Max pooling
        x = torch.max(x, 2, keepdim=True)[0]
        x = x.view(-1, 1024)

        x = F.relu(self.bn4(self.fc1(x)))
        x = F.relu(self.bn5(self.fc2(x)))
        x = self.fc3(x)

        # Apply to all points (simple broadcast) - for segmentation we need per-point features
        # This is a simplified classification head on global features.
        # For segmentation, we'd concatenate global features back to local points.
        # But for this scaffold, we'll output global class scores (B, num_classes).

        return F.log_softmax(x, dim=1)

class PointTransformer(nn.Module):
    """
    Placeholder for a more complex Point Transformer model.
    """
    def __init__(self, in_channels=3, num_classes=10):
        super().__init__()
        self.net = PointNet(in_channels, num_classes) # Fallback to PointNet for scaffold

    def forward(self, x):
        return self.net(x)
