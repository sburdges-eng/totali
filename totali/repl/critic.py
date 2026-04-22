"""Critic node helper for script retries against the Civil 3D REPL bridge."""

from __future__ import annotations

from typing import Any, Mapping

from .civil3d_repl import Civil3DREPLBridge


def totali_critic_node(state: Mapping[str, Any], repl_bridge: Civil3DREPLBridge) -> dict[str, Any]:
    """
    Execute generated script and return deterministic feedback for correction loops.

    Required state keys:
    - generated_code: str
    Optional state keys:
    - attempt: int (defaults to 1)
    """

    if "generated_code" not in state:
        raise KeyError("state is missing required key: generated_code")

    script = str(state["generated_code"])
    attempt = int(state.get("attempt", 1))
    result = repl_bridge.execute_script(script)

    if result.success:
        return {
            "status": "PASS",
            "message": f"Execution successful. stdout: {result.stdout}",
            "telemetry": result.telemetry,
        }

    feedback_prompt = (
        f"Your script crashed during execution on attempt {attempt}.\n"
        "The drawing state was rolled back safely.\n"
        f"Traceback:\n{result.error_traceback}\n"
        f"Standard Output before crash:\n{result.stdout}\n"
        "Analyze the traceback. If it is a topology error, fix the spatial math. "
        "If it is an API error, fix the syntax. Rewrite the script."
    )
    return {
        "status": "FAIL",
        "feedback": feedback_prompt,
        "error_traceback": result.error_traceback,
        "attempt": attempt + 1,
    }
