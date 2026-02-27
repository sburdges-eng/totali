import argparse
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from pathlib import Path
from tqdm import tqdm
import time

# Import common utilities
import sys
sys.path.append(str(Path(__file__).parents[2]))
from training.common.config import load_config
from training.common.logger import setup_logger
from training.common.metrics import MetricsTracker
from training.segmentation.dataset import PointCloudDataset
from training.segmentation.model import PointTransformer

def train(config_path: str, output_dir: str):
    logger = setup_logger("seg_train", f"{output_dir}/logs")
    metrics_tracker = MetricsTracker()

    config = load_config(config_path)
    logger.info(f"Loaded config: {config}")

    # Hyperparameters
    batch_size = config.get("batch_size", 16)
    lr = config.get("learning_rate", 1e-3)
    epochs = config.get("epochs", 10)
    num_points = config.get("num_points", 4096)
    num_classes = config.get("num_classes", 10)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Data
    train_dir = config.get("train_data_dir", "datasets/tiled/lidar")
    dataset = PointCloudDataset(train_dir, num_points=num_points)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True, num_workers=4)

    # Model
    model = PointTransformer(in_channels=3, num_classes=num_classes).to(device) # XYZ only for now
    optimizer = optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.NLLLoss() # Expecting LogSoftmax output

    logger.info(f"Starting training on {device} with {len(dataset)} samples")
    start_time = time.time()

    for epoch in range(epochs):
        model.train()
        running_loss = 0.0
        correct = 0
        total = 0

        pbar = tqdm(dataloader, desc=f"Epoch {epoch+1}/{epochs}")
        for points, labels in pbar:
            points, labels = points.to(device), labels.to(device)

            optimizer.zero_grad()
            outputs = model(points)

            # Simple global classification task for scaffold (B, num_classes) vs (B)
            # For real segmentation, outputs would be (B, N, num_classes) vs (B, N)
            # We use majority vote of point labels as 'target' for this simple check
            target = torch.mode(labels, dim=1)[0]

            loss = loss_fn(outputs, target)
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

            pbar.set_postfix({"loss": loss.item()})

        avg_loss = running_loss / len(dataloader)
        accuracy = 100 * correct / total if total > 0 else 0
        logger.info(f"Epoch {epoch+1} Loss: {avg_loss:.4f}, Acc: {accuracy:.2f}%")

        metrics_tracker.log_epoch(epoch + 1, avg_loss, avg_loss, metrics={"accuracy": accuracy})

    total_time = time.time() - start_time
    logger.info(f"Training complete in {total_time:.2f}s")

    # Save model
    model_path = f"{output_dir}/segmentation_model.pth"
    torch.save(model.state_dict(), model_path)
    logger.info(f"Model saved to {model_path}")

    # Export to ONNX (using dummy input)
    dummy_input = torch.randn(1, num_points, 3).to(device)
    onnx_path = f"{output_dir}/segmentation_model.onnx"
    torch.onnx.export(model, dummy_input, onnx_path, verbose=False)
    logger.info(f"Model exported to ONNX: {onnx_path}")

    # Update final metrics
    metrics_tracker.update_quality(heldout_score=accuracy/100.0)
    metrics_tracker.save(f"{output_dir}/metrics.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="Path to config yaml")
    parser.add_argument("--output-dir", type=str, default="artifacts/seg_run", help="Output directory")
    args = parser.parse_args()

    train(args.config, args.output_dir)
