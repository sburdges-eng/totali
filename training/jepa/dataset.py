import numpy as np
import torch
from torch.utils.data import Dataset
from pathlib import Path
from typing import List, Optional, Tuple
import logging

class JEPADataset(Dataset):
    """
    Dataset for JEPA training using heightmaps (.npy files).
    """
    def __init__(self, data_dir: str, transform=None, cache_data: bool = False):
        self.data_dir = Path(data_dir)
        self.transform = transform
        self.files = sorted(list(self.data_dir.glob("*.npy")))
        self.cache_data = cache_data
        self.cache = {}

        if len(self.files) == 0:
            logging.warning(f"No .npy files found in {data_dir}")

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        file_path = self.files[idx]

        if self.cache_data and idx in self.cache:
            data = self.cache[idx]
        else:
            try:
                # Load heightmap: shape (H, W, 2) -> (elevation, density)
                data = np.load(file_path).astype(np.float32)

                # Normalize or preprocess if needed
                # Ideally normalization stats should be computed globally
                # For now, we replace NaNs with 0 (or a specific value for 'unknown')
                data = np.nan_to_num(data, nan=0.0)

                # Transpose to (C, H, W) for PyTorch
                data = data.transpose(2, 0, 1)

                if self.cache_data:
                    self.cache[idx] = data
            except Exception as e:
                logging.error(f"Error loading {file_path}: {e}")
                # Return a zero tensor in case of error to keep batching working
                return torch.zeros((2, 100, 100), dtype=torch.float32)

        tensor = torch.from_numpy(data)

        if self.transform:
            tensor = self.transform(tensor)

        return tensor
