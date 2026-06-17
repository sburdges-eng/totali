"""Unit tests for the Civil 3D REPL bridge package."""

from __future__ import annotations

import json
import socket
from typing import Any

import pytest

from totali.repl import Civil3DREPLBridge, Civil3DReplClient, totali_critic_node
from totali.repl.contracts import (
    CONTRACT_TOP_LEVEL_KEYS,
    build_contract_payload,
    validate_contract_payload,
)


class _FakeDoc:
    def __init__(self) -> None:
        self.ModelSpace = object()
        self.start_calls = 0
        self.end_calls = 0
        self.commands: list[str] = []

    def StartUndoMark(self) -> None:  # noqa: N802
        self.start_calls += 1

    def EndUndoMark(self) -> None:  # noqa: N802
        self.end_calls += 1

    def SendCommand(self, value: str) -> None:  # noqa: N802
        self.commands.append(value)


class _FakeSocket:
    def __init__(self, response_bytes: bytes) -> None:
        self._response_bytes = response_bytes
        self.sent = b""
        self.shutdown_called = False

    def __enter__(self) -> "_FakeSocket":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def sendall(self, payload: bytes) -> None:
        self.sent += payload

    def shutdown(self, how: int) -> None:
        self.shutdown_called = how == socket.SHUT_WR

    def recv(self, size: int) -> bytes:
        if not self._response_bytes:
            return b""
        chunk = self._response_bytes[:size]
        self._response_bytes = self._response_bytes[size:]
        return chunk


def test_contract_payload_has_canonical_key_order() -> None:
    payload = build_contract_payload(
        artifact_type="civil3d_repl_request",
        metadata={"seed": "abc"},
        data={"script": "print('ok')"},
        invariants=("script_utf8_encoded",),
    )
    assert tuple(payload.keys()) == CONTRACT_TOP_LEVEL_KEYS
    assert validate_contract_payload(payload) == []


def test_execute_script_success_populates_stdout_and_telemetry() -> None:
    bridge = Civil3DREPLBridge(connect=False)
    fake_doc = _FakeDoc()
    bridge.doc = fake_doc

    result = bridge.execute_script("print('ok')\nagent_telemetry['handle'] = '0x1'")

    assert result.success is True
    assert result.stdout == "ok"
    assert result.telemetry == {"handle": "0x1"}
    assert fake_doc.start_calls == 1
    assert fake_doc.end_calls == 1
    assert fake_doc.commands == []


def test_execute_script_runtime_error_triggers_rollback() -> None:
    bridge = Civil3DREPLBridge(connect=False)
    fake_doc = _FakeDoc()
    bridge.doc = fake_doc

    result = bridge.execute_script("raise RuntimeError('boom')")

    assert result.success is False
    assert "Python Runtime Error" in (result.error_traceback or "")
    assert fake_doc.start_calls == 1
    assert fake_doc.end_calls == 1
    assert fake_doc.commands == ["_U\n"]


def test_send_command_is_blocked_by_default() -> None:
    bridge = Civil3DREPLBridge(connect=False)
    bridge.doc = _FakeDoc()

    result = bridge.execute_script('doc.SendCommand("LINE ")')

    assert result.success is False
    assert "Safety policy violation" in (result.error_traceback or "")


def test_critic_node_emits_retry_feedback_on_failure() -> None:
    bridge = Civil3DREPLBridge(connect=False)
    bridge.doc = _FakeDoc()

    output = totali_critic_node(
        {"generated_code": "raise RuntimeError('bad')", "attempt": 2},
        bridge,
    )

    assert output["status"] == "FAIL"
    assert output["attempt"] == 3
    assert "Traceback:" in output["feedback"]


def test_client_normalizes_legacy_listener_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy_response = json.dumps({"status": "success", "handles": ["0x2F4"]}).encode("utf-8")
    fake_socket = _FakeSocket(legacy_response)

    def _fake_create_connection(address: tuple[str, int], timeout: float) -> _FakeSocket:
        assert address == ("127.0.0.1", 5050)
        assert timeout == 3.0
        return fake_socket

    monkeypatch.setattr(socket, "create_connection", _fake_create_connection)

    client = Civil3DReplClient(timeout_sec=3.0)
    result = client.execute_and_wait("print('hello')")

    assert result["status"] == "success"
    assert result["handles"] == ["0x2F4"]
    assert result["schemaVersion"] == "1.0.0"
    assert "legacy_payload_normalized" in result["invariants"]
    assert fake_socket.shutdown_called is True

    request_payload = json.loads(fake_socket.sent.decode("utf-8"))
    assert list(request_payload.keys())[0] == "schemaVersion"
    assert request_payload["artifactType"] == "civil3d_repl_request"


def test_client_connection_refused_returns_deterministic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_connection_refused(address: tuple[str, int], timeout: float) -> _FakeSocket:
        raise ConnectionRefusedError("no listener")

    monkeypatch.setattr(socket, "create_connection", _raise_connection_refused)

    client = Civil3DReplClient()
    result = client.execute_and_wait("print('hello')")
    assert result["status"] == "error"
    assert "listener is not running" in result["message"].lower()
