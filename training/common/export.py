import torch
import onnx
from onnxsim import simplify
from pathlib import Path
import logging

def export_to_onnx(model: torch.nn.Module, input_shape: tuple, output_path: str, dynamic_axes: dict = None):
    """
    Export a PyTorch model to ONNX and simplify it.
    """
    model.eval()
    dummy_input = torch.randn(input_shape).to(next(model.parameters()).device)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        torch.onnx.export(
            model,
            dummy_input,
            output_path,
            verbose=False,
            input_names=['input'],
            output_names=['output'],
            dynamic_axes=dynamic_axes or {'input': {0: 'batch_size'}, 'output': {0: 'batch_size'}}
        )
        logging.info(f"Model exported to {output_path}")

        # Simplify using onnx-simplifier
        try:
            onnx_model = onnx.load(output_path)
            model_simp, check = simplify(onnx_model)
            if check:
                onnx.save(model_simp, output_path)
                logging.info(f"Model simplified successfully: {output_path}")
            else:
                logging.warning("Simplification check failed, keeping original model")
        except Exception as e:
            logging.warning(f"ONNX simplification failed (optional): {e}")

    except Exception as e:
        logging.error(f"Error exporting to ONNX: {e}")
        raise
