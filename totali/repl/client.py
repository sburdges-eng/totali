"""TCP client for the Civil 3D listener bridge."""

from __future__ import annotations

import json
import socket
import uuid
from typing import Any

from .contracts import build_contract_payload, canonical_json, validate_contract_payload


class Civil3DReplClient:
    """Deterministic request/response transport for a local Civil 3D listener."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5050, timeout_sec: float = 30.0) -> None:
        self.host = host
        self.port = int(port)
        self.timeout_sec = float(timeout_sec)

    def _build_request_contract(
        self,
        script_payload: str,
        *,
        seed: str | None = None,
        allow_send_command: bool = False,
    ) -> dict[str, Any]:
        request_seed = seed or uuid.uuid4().hex
        return build_contract_payload(
            artifact_type="civil3d_repl_request",
            invariants=("script_utf8_encoded", "request_seed_present"),
            metadata={
                "seed": request_seed,
                "timeoutSec": self.timeout_sec,
            },
            data={
                "script": script_payload,
                "allowSendCommand": bool(allow_send_command),
            },
        )

    def _recv_all(self, conn: socket.socket) -> bytes:
        chunks: list[bytes] = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    def _normalize_response_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "schemaVersion" in payload:
            errors = validate_contract_payload(payload, expected_artifact_type="civil3d_repl_result")
            if errors:
                raise ValueError("Invalid result contract: " + "; ".join(errors))
            data = payload.get("data", {})
            if not isinstance(data, dict):
                raise ValueError("Invalid result contract: `data` must be an object")

            success = bool(data.get("success", False))
            message = str(data.get("errorTraceback", "")) if not success else "Execution successful."
            handles = data.get("handles", [])
            telemetry = data.get("telemetry", {})
            stdout = str(data.get("stdout", ""))
            return {
                "success": success,
                "message": message,
                "handles": handles if isinstance(handles, list) else [],
                "telemetry": telemetry if isinstance(telemetry, dict) else {},
                "stdout": stdout,
                "contract": payload,
            }

        legacy_status = payload.get("status")
        if legacy_status not in {"success", "error"}:
            raise ValueError("Listener response missing `status` and no schemaVersion contract envelope.")

        success = legacy_status == "success"
        message = str(payload.get("message", ""))
        handles = payload.get("handles", [])
        telemetry = payload.get("telemetry", {})
        stdout = str(payload.get("stdout", ""))

        normalized_contract = build_contract_payload(
            artifact_type="civil3d_repl_result",
            invariants=("legacy_payload_normalized",),
            metadata={"source": "legacy_listener"},
            data={
                "success": success,
                "stdout": stdout,
                "errorTraceback": "" if success else message,
                "telemetry": telemetry if isinstance(telemetry, dict) else {},
                "handles": handles if isinstance(handles, list) else [],
            },
        )
        return {
            "success": success,
            "message": "Execution successful." if success else message,
            "handles": handles if isinstance(handles, list) else [],
            "telemetry": telemetry if isinstance(telemetry, dict) else {},
            "stdout": stdout,
            "contract": normalized_contract,
        }

    def execute_and_wait(
        self,
        script_payload: str,
        *,
        seed: str | None = None,
        allow_send_command: bool = False,
    ) -> dict[str, Any]:
        request_contract = self._build_request_contract(
            script_payload,
            seed=seed,
            allow_send_command=allow_send_command,
        )
        outbound = canonical_json(request_contract).encode("utf-8")

        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout_sec) as conn:
                conn.sendall(outbound)
                conn.shutdown(socket.SHUT_WR)
                raw = self._recv_all(conn)
        except ConnectionRefusedError:
            return {
                "status": "error",
                "message": "Civil 3D listener is not running. Start the listener inside Civil 3D first.",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }
        except (socket.timeout, TimeoutError):
            return {
                "status": "error",
                "message": "Civil 3D listener timed out while executing script.",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }
        except OSError as exc:
            return {
                "status": "error",
                "message": f"Transport error while contacting Civil 3D listener: {exc}",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }

        if not raw:
            return {
                "status": "error",
                "message": "Civil 3D listener returned an empty response.",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }

        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {
                "status": "error",
                "message": f"Invalid response from Civil 3D listener: {exc}",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }

        if not isinstance(payload, dict):
            return {
                "status": "error",
                "message": "Invalid response from Civil 3D listener: root payload must be an object.",
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }

        try:
            normalized = self._normalize_response_payload(payload)
        except ValueError as exc:
            return {
                "status": "error",
                "message": str(exc),
                "stdout": "",
                "handles": [],
                "telemetry": {},
                "schemaVersion": request_contract["schemaVersion"],
                "invariants": request_contract["invariants"],
                "contract": {},
            }

        return {
            "status": "success" if normalized["success"] else "error",
            "message": normalized["message"],
            "stdout": normalized["stdout"],
            "handles": normalized["handles"],
            "telemetry": normalized["telemetry"],
            "schemaVersion": normalized["contract"]["schemaVersion"],
            "invariants": normalized["contract"]["invariants"],
            "contract": normalized["contract"],
        }
