import os

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from totali.models.projection import TotaliMultimodalProjector

# --- Prompts ---
CODER_SYSTEM_PROMPT = """
You are TOTaLi, an Autonomous CAD Agent operating inside Civil 3D.
You have been provided with 64 spatial tokens representing the 3D topography of the site.
Output ONLY valid Python code utilizing the `civil3d_api`. Do not output markdown or explanations.
"""

CRITIC_RETRY_PROMPT = """
Your previous script failed execution in the Civil 3D REPL.
Task: {task}

<failed_script>
{failed_script}
</failed_script>

<civil3d_error>
{traceback}
</civil3d_error>

Diagnose the geometric or syntactic failure and output the corrected script.
"""


class GeometricCoderAgent:
    def __init__(self, model_path: str = "meta-llama/Meta-Llama-3-70B-Instruct"):
        """Initializes the LLM, the Tokenizer, and the Multimodal Projector."""
        print(f"Loading TOTaLi Base Model: {model_path}")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        # Load the base text model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.llm = AutoModelForCausalLM.from_pretrained(model_path).to(self.device)

        # Load our custom-trained 3D Projector weights (from Stage 1 Pretraining)
        self.projector = TotaliMultimodalProjector().to(self.device)
        # Mock load: self.projector.load_state_dict(torch.load("weights/projector_v1.pt"))

    def _prepare_multimodal_inputs(self, prompt: str, tensor_path: str) -> torch.Tensor:
        """
        The Early Fusion Core: Bypasses standard text generation to manually
        concatenate spatial embeddings with text embeddings.
        """
        # 1. Load the Zero-Centered (8192 x 3) Spatial Tensor
        if not os.path.exists(tensor_path):
            raise FileNotFoundError(f"Spatial context missing: {tensor_path}")

        # Shape: (1, 8192, 3)
        raw_point_cloud = torch.load(tensor_path).to(self.device)

        # 2. Project into LLM Latent Space
        # Shape: (1, 64, 4096) -> 64 tokens, 4096 dimensions
        spatial_embeds = self.projector(raw_point_cloud)

        # 3. Tokenize and Embed the Text Prompt
        full_prompt = f"{CODER_SYSTEM_PROMPT}\nUSER TASK: {prompt}"
        text_tokens = self.tokenizer(full_prompt, return_tensors="pt").to(self.device)

        # Get raw text embeddings from the LLM's first layer
        # Shape: (1, Seq_Len, 4096)
        text_embeds = self.llm.get_input_embeddings()(text_tokens.input_ids)

        # 4. CONCATENATE: Spatial Context FIRST, Text Instructions SECOND.
        # This ensures the model "sees" the terrain before reading what to do with it.
        # Final Shape: (1, 64 + Seq_Len, 4096)
        inputs_embeds = torch.cat([spatial_embeds, text_embeds], dim=1)

        return inputs_embeds

    def generate(self, task: str, spatial_tensor_path: str) -> str:
        """Standard Generation Route (Node B in LangGraph)."""
        inputs_embeds = self._prepare_multimodal_inputs(task, spatial_tensor_path)

        # Generate the script based on the fused embeddings
        # Note: We pass inputs_embeds instead of input_ids
        outputs = self.llm.generate(
            inputs_embeds=inputs_embeds,
            max_new_tokens=1024,
            temperature=0.1,  # Keep generation highly deterministic
            stop_strings=["```"],
        )

        return self.tokenizer.decode(outputs[0], skip_special_tokens=True)

    def fix(self, task: str, failed_script: str, traceback: str, spatial_tensor_path: str) -> str:
        """Critic Correction Route (The self-healing loop)."""
        critic_prompt = CRITIC_RETRY_PROMPT.format(
            task=task,
            failed_script=failed_script,
            traceback=traceback,
        )

        # We re-inject the spatial tensor so the model can verify the geometry
        # while fixing the error (e.g., checking if points are actually collinear).
        inputs_embeds = self._prepare_multimodal_inputs(critic_prompt, spatial_tensor_path)

        outputs = self.llm.generate(
            inputs_embeds=inputs_embeds,
            max_new_tokens=1024,
            temperature=0.2,  # Slightly higher variance to break out of syntax loops
        )

        return self.tokenizer.decode(outputs[0], skip_special_tokens=True)


# Example Instantiation for the Orchestrator
coder_agent = GeometricCoderAgent()
