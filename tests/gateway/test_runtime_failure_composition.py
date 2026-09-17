"""Execute selected real AST boundaries without importing gateway startup.

This checks source composition with synthetic state, not full runtime loading.
The full gateway's providers, hooks and persistence remain root verification.
"""
import ast
import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.message_audience import OutputClass
from gateway.config import Platform
from gateway.message_failure import SAFE_FAILURE_TEXT, normalize_agent_response, requires_safe_failure


SOURCE = Path(__file__).resolve().parents[2] / "gateway/run.py"


def tree():
    return ast.parse(SOURCE.read_text())


def execute_function(node, namespace):
    node.decorator_list = []
    node.returns = None
    for argument in node.args.posonlyargs + node.args.args + node.args.kwonlyargs:
        argument.annotation = None
    module = ast.Module(body=[node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(SOURCE), "exec"), namespace)
    return namespace[node.name]


@pytest.mark.parametrize("state", [{"failed": True}, {"partial": True}, {"error": "synthetic diagnostic"}, {"completed": False}])
def test_real_result_projection_retains_failure_for_final_normalization(state):
    # Compile the actual result envelope from run_sync, including every field.
    envelope = next(n for n in ast.walk(tree()) if isinstance(n, ast.Dict)
        and {"final_response", "history_offset", "agent_persisted"}.issubset(
            {k.value for k in n.keys if isinstance(k, ast.Constant)}))
    namespace = {n.id: None for n in ast.walk(envelope) if isinstance(n, ast.Name)}
    result = {"final_response": "synthetic private failure", **state}
    namespace.update(result=result, result_holder=[result], tools_holder=[[]], final_response=result["final_response"])
    projected = eval(compile(ast.Expression(envelope), str(SOURCE), "eval"), namespace)
    projected["already_sent"] = True
    assert requires_safe_failure(projected)
    assert normalize_agent_response(projected, projected["final_response"]) == SAFE_FAILURE_TEXT


def test_external_heartbeat_returns_before_adapter_or_activity_access():
    node = next(n for n in ast.walk(tree()) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_notify_long_running")
    namespace = {"_NOTIFY_INTERVAL": 1, "_gateway_output_allowed": lambda *args: False,
                 "self": object(), "source": object(), "OutputClass": OutputClass}
    asyncio.run(execute_function(node, namespace)())


@pytest.mark.parametrize("result", [{"failed": True, "final_response": "synthetic private path"}, {"partial": True, "error": "synthetic error"}])
def test_background_failure_is_normalized_before_media_or_success_header(result):
    function = next(n for n in ast.walk(tree()) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_run_background_task")
    # Begin just after the executor returns. Preserve all actual delivery
    # statements so raw failure text, media extraction and success headers
    # cannot silently reappear in this branch.
    assignment = next(n for n in ast.walk(function) if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "result" for t in n.targets)
        and isinstance(n.value, ast.Await))
    block = next(n for n in ast.walk(function) if hasattr(n, "body") and isinstance(n.body, list) and assignment in n.body)
    statements = block.body[block.body.index(assignment)+1:]
    adapter = SimpleNamespace(send=AsyncMock(), extract_media=lambda text: pytest.fail("failure reached media extraction"))
    namespace = {"result": result, "adapter": adapter, "source": SimpleNamespace(chat_id="CSAME"),
        "_thread_metadata": {"thread_id": "thread"}, "logger": logging.getLogger(__name__),
        "SAFE_FAILURE_TEXT": SAFE_FAILURE_TEXT, "requires_safe_failure": requires_safe_failure,
        "normalize_agent_response": normalize_agent_response, "OutputClass": OutputClass}
    node = ast.AsyncFunctionDef(name="deliver", args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]), body=statements, decorator_list=[])
    asyncio.run(execute_function(node, namespace)())
    adapter.send.assert_awaited_once()
    assert adapter.send.call_args.kwargs["content"] == SAFE_FAILURE_TEXT
    assert adapter.send.call_args.kwargs["metadata"]["thread_id"] == "thread"


def test_slack_confirmation_suppression_cannot_become_plain_text_fallback():
    node = next(n for n in ast.walk(tree()) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_request_slash_confirm")
    namespace = {"_gateway_output_allowed": lambda *args: False, "Platform": Platform, "OutputClass": OutputClass}
    function = execute_function(node, namespace)
    result = asyncio.run(function(object(), event=SimpleNamespace(source=SimpleNamespace(platform=Platform.SLACK)),
        command="synthetic", title="Synthetic", message="synthetic private operational content", handler=AsyncMock()))
    assert result is None
