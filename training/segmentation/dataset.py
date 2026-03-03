import numpy as np
import torch
from torch.utils.data import Dataset
from pathlib import Path
import laspy
import logging

class PointCloudDataset(Dataset):
    """
    Dataset for Point Cloud Segmentation using LAS/LAZ tiles.
    """
    def __init__(self, data_dir: str, num_points: int = 4096, transform=None):
        self.data_dir = Path(data_dir)
        self.files = sorted(list(self.data_dir.glob("*.laz"))) + sorted(list(self.data_dir.glob("*.las")))
        self.num_points = num_points
        self.transform = transform

        if len(self.files) == 0:
            logging.warning(f"No LAS/LAZ files found in {data_dir}")

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        file_path = self.files[idx]

        try:
            with laspy.open(str(file_path)) as f:
                las = f.read()
                points = np.vstack((las.x, las.y, las.z)).transpose() # (N, 3)

                # Use intensity if available
                if hasattr(las, 'intensity'):
                    intensity = las.intensity.astype(np.float32)
                    points = np.hstack((points, intensity[:, None])) # (N, 4)

                # Use classification as labels if available
                if hasattr(las, 'classification'):
                    labels = las.classification.astype(np.int64)
                else:
                    labels = np.zeros(len(points), dtype=np.int64)

            # Sampling (Random for now, could be voxel grid)
            if len(points) > self.num_points:
                choice = np.random.choice(len(points), self.num_points, replace=False)
                points = points[choice]
                labels = labels[choice]
            else:
                # Pad with zeros if fewer points
                pad_len = self.num_points - len(points)
                points = np.pad(points, ((0, pad_len), (0, 0)), mode='constant')
                labels = np.pad(labels, (0, pad_len), mode='constant', constant_values=-1) # -1 ignore index

        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
            return torch.zeros((self.num_points, 3)), torch.zeros((self.num_points), dtype=torch.long)

        points = torch.from_numpy(points).float()
        labels = torch.from_numpy(labels).long()

        if self.transform:
            points, labels = self.transform(points, labels)

        return points, labels
