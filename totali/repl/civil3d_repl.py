"""Civil 3D REPL bridge with deterministic safety checks and rollback behavior."""

from __future__ import annotations

import ast
import io
import math
import sys
import traceback
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

from .contracts import build_contract_payload

_BLOCKED_BUILTINS = {"open", "eval", "exec", "compile", "__import__", "input"}

_SAFE_BUILTINS: dict[str, Any] = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "pow": pow,
    "print": print,
    "range": range,
    "round": round,
    "set": set,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
    "Exception": Exception,
    "RuntimeError": RuntimeError,
    "ValueError": ValueError,
}


class BridgeConnectionError(RuntimeError):
    """Raised when the bridge cannot connect to an active Civil 3D COM server."""


class ScriptSafetyError(ValueError):
    """Raised when a generated script violates REPL safety policy."""


@dataclass(slots=True)
class WorldTransform:
    """Deterministic transform for normalized spatial coordinates."""

    centroid_x: float = 0.0
    centroid_y: float = 0.0
    centroid_z: float = 0.0
    scale_xy: float = 1.0
    scale_z: float = 1.0

    def to_world(self, x: float, y: float, z: float) -> tuple[float, float, float]:
        return (
            float(self.centroid_x + (float(x) * self.scale_xy)),
            float(self.centroid_y + (float(y) * self.scale_xy)),
            float(self.centroid_z + (float(z) * self.scale_z)),
        )


class REPLResult(BaseModel):
    """Result payload for an in-process script execution."""

    schemaVersion: str = "1.0.0"
    success: bool
    stdout: str
    error_traceback: str | None = None
    telemetry: dict[str, Any] = Field(default_factory=dict)
    invariants: list[str] = Field(
        default_factory=lambda: [
            "schema_version_required",
            "deterministic_key_order",
            "atomic_rollback_on_failure",
            "modal_commands_blocked_by_default",
        ]
    )

    def as_contract(self) -> dict[str, Any]:
        return build_contract_payload(
            artifact_type="civil3d_repl_result",
            invariants=self.invariants,
            metadata={"success": self.success},
            data={
                "success": self.success,
                "stdout": self.stdout,
                "errorTraceback": self.error_traceback or "",
                "telemetry": self.telemetry,
            },
        )


class Civil3DREPLBridge:
    """Stateful sandbox that executes generated Python against Civil 3D COM objects."""

    _VERSION_MAP = {"2022": "13.4", "2023": "13.5", "2024": "13.6"}

    def __init__(
        self,
        target_year: str = "2024",
        *,
        connect: bool = True,
        allow_send_command: bool = False,
    ) -> None:
        self.aecc_version = self._VERSION_MAP.get(target_year, "13.6")
        self.allow_send_command = allow_send_command

        self.acad_app: Any | None = None
        self.civil_app: Any | None = None
        self.doc: Any | None = None
        self._pythoncom: Any | None = None
        self._modal_baseline: dict[str, Any] = {}
        self._world_transform = WorldTransform()

        if connect:
            self._connect()

    def _connect(self) -> None:
        """Bind to an active Civil 3D session through COM."""
        try:
            import pythoncom  # type: ignore
            import win32com.client  # type: ignore
        except ImportError as exc:
            raise BridgeConnectionError(
                "pywin32 is required for Civil 3D COM access (pythoncom, win32com.client)."
            ) from exc

        try:
            pythoncom.CoInitialize()
            self._pythoncom = pythoncom
            self.acad_app = win32com.client.Dispatch("AutoCAD.Application")
            self.doc = getattr(self.acad_app, "ActiveDocument", None)
            if self.doc is None:
                raise BridgeConnectionError("AutoCAD has no active document.")

            self._capture_modal_baseline()
            self._disable_modal_dialogs()

            aecc_progid = f"AeccXUiLand.AeccApplication.{self.aecc_version}"
            self.civil_app = self.acad_app.GetInterfaceObject(aecc_progid)
        except Exception as exc:
            raise BridgeConnectionError(
                "Could not attach to Civil 3D COM server. Ensure Civil 3D is running with an open drawing."
            ) from exc

    def _capture_modal_baseline(self) -> None:
        for variable in ("FILEDIA", "CMDDIA", "EXPERT"):
            value = self._read_doc_variable(variable)
            if value is not None:
                self._modal_baseline[variable] = value

    def _disable_modal_dialogs(self) -> None:
        self._write_doc_variable("FILEDIA", 0)
        self._write_doc_variable("CMDDIA", 0)
        self._write_doc_variable("EXPERT", 5)

    def _read_doc_variable(self, variable: str) -> Any | None:
        if self.doc is None:
            return None
        getter = getattr(self.doc, "GetVariable", None)
        if callable(getter):
            try:
                return getter(variable)
            except Exception:
                return None
        return None

    def _write_doc_variable(self, variable: str, value: Any) -> None:
        if self.doc is None:
            return
        setter = getattr(self.doc, "SetVariable", None)
        if callable(setter):
            try:
                setter(variable, value)
            except Exception:
                return

    def close(self) -> None:
        """Restore toggled CAD variables and uninitialize COM apartment."""
        for variable, value in self._modal_baseline.items():
            self._write_doc_variable(variable, value)
        if self._pythoncom is not None:
            try:
                self._pythoncom.CoUninitialize()
            except Exception:
                pass
            self._pythoncom = None

    def configure_world_transform(
        self,
        *,
        centroid: tuple[float, float, float],
        scale_xy: float,
        scale_z: float | None = None,
    ) -> None:
        z_scale = scale_xy if scale_z is None else scale_z
        self._world_transform = WorldTransform(
            centroid_x=float(centroid[0]),
            centroid_y=float(centroid[1]),
            centroid_z=float(centroid[2]),
            scale_xy=float(scale_xy),
            scale_z=float(z_scale),
        )

    def totali_to_world(self, x: float, y: float, z: float) -> tuple[float, float, float]:
        return self._world_transform.to_world(x, y, z)

    def _validate_script(self, script: str) -> None:
        try:
            tree = ast.parse(script, mode="exec")
        except SyntaxError as exc:
            raise ScriptSafetyError(f"Syntax error on line {exc.lineno}: {exc.msg}") from exc

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                raise ScriptSafetyError("Import statements are blocked in this REPL sandbox.")
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in _BLOCKED_BUILTINS:
                    raise ScriptSafetyError(
                        f"Blocked builtin call `{node.func.id}` on line {node.lineno}."
                    )
                if isinstance(node.func, ast.Attribute) and node.func.attr == "SendCommand":
                    if not self.allow_send_command:
                        raise ScriptSafetyError(
                            "SendCommand is blocked by policy to prevent interactive deadlocks."
                        )
                    if not self._is_safe_send_command(node):
                        raise ScriptSafetyError(
                            "SendCommand requires a constant command string ending with newline."
                        )

    def _is_safe_send_command(self, node: ast.Call) -> bool:
        if not node.args:
            return False
        first_arg = node.args[0]
        if not isinstance(first_arg, ast.Constant):
            return False
        if not isinstance(first_arg.value, str):
            return False
        return first_arg.value.endswith("\n")

    def _build_namespace(self) -> dict[str, Any]:
        if self.doc is None:
            raise BridgeConnectionError("Bridge is not connected to an active document.")
        return {
            "__builtins__": _SAFE_BUILTINS,
            "acad": self.acad_app,
            "civil": self.civil_app,
            "doc": self.doc,
            "ms": getattr(self.doc, "ModelSpace", None),
            "math": math,
            "agent_telemetry": {},
            "totali_to_world": self.totali_to_world,
        }

    def _is_com_error(self, exc: Exception) -> bool:
        if self._pythoncom is None:
            return exc.__class__.__name__ == "com_error"
        com_error_type = getattr(self._pythoncom, "com_error", None)
        return bool(com_error_type) and isinstance(exc, com_error_type)

    def _parse_com_error(self, error: Exception) -> str:
        try:
            hresult, msg, excepinfo, _ = error.args
            desc = excepinfo[2] if excepinfo else msg
            desc_text = str(desc)

            lowered = desc_text.lower()
            if "intersect" in lowered:
                return f"Topology Error: Geometry self-intersects. Details: {desc_text}"
            if "invalid input" in lowered:
                return f"API Syntax Error: Invalid parameters passed to method. Details: {desc_text}"
            return f"Civil 3D API Exception [HRESULT {hresult}]: {desc_text}"
        except Exception:
            return f"Unknown COM Error: {error}"

    def _start_undo_mark(self) -> bool:
        if self.doc is None:
            return False
        start_undo_mark = getattr(self.doc, "StartUndoMark", None)
        if callable(start_undo_mark):
            start_undo_mark()
            return True
        return False

    def _end_undo_mark(self) -> None:
        if self.doc is None:
            return
        end_undo_mark = getattr(self.doc, "EndUndoMark", None)
        if callable(end_undo_mark):
            end_undo_mark()

    def _rollback_document(self) -> None:
        if self.doc is None:
            return
        send_command = getattr(self.doc, "SendCommand", None)
        if callable(send_command):
            send_command("_U\n")

    def execute_script(self, llm_script: str) -> REPLResult:
        """
        Execute one generated Python script in an isolated namespace.
        Any failure triggers rollback and returns structured traceback text.
        """
        if self.doc is None:
            return REPLResult(
                success=False,
                stdout="",
                error_traceback="Bridge is not connected. Start Civil 3D and initialize the COM bridge.",
            )

        try:
            self._validate_script(llm_script)
        except ScriptSafetyError as exc:
            return REPLResult(
                success=False,
                stdout="",
                error_traceback=f"Safety policy violation: {exc}",
            )

        namespace = self._build_namespace()
        captured_stdout = io.StringIO()
        original_stdout = sys.stdout
        undo_open = False
        success = False
        error_msg: str | None = None

        try:
            undo_open = self._start_undo_mark()
            sys.stdout = captured_stdout
            exec(compile(llm_script, "<totali_repl>", "exec"), namespace, namespace)
            success = True
            if undo_open:
                self._end_undo_mark()
                undo_open = False
        except Exception as exc:
            if undo_open:
                self._end_undo_mark()
                undo_open = False
            self._rollback_document()

            if self._is_com_error(exc):
                error_msg = self._parse_com_error(exc)
            else:
                error_msg = f"Python Runtime Error: {exc}\n{traceback.format_exc()}"
        finally:
            sys.stdout = original_stdout
            if undo_open:
                self._end_undo_mark()

        telemetry = namespace.get("agent_telemetry", {})
        if not isinstance(telemetry, dict):
            telemetry = {}

        return REPLResult(
            success=success,
            stdout=captured_stdout.getvalue().strip(),
            error_traceback=error_msg,
            telemetry=telemetry,
        )

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            return
