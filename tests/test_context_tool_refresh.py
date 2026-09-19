"""Selected real installed callables with inert dependency seams, not full Gateway.

Registry and candidate helper load as whole modules. Large gateway/executor/MCP
functions are compiled unchanged from their pinned source AST, with explicit
fake UI/config/persistence/provider seams. No live plugins or providers load.
"""
from __future__ import annotations

import ast
import contextlib
import contextvars
import importlib.util
import json
import logging
from pathlib import Path
import sys
import threading
import time
import types
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
# Pinned harness keeps the repo under a source/ sibling plus a captured copy at
# native-caller-map/source; repo CI has plain repo-root paths.
SOURCE = BASE / "source"
if not SOURCE.exists():
    SOURCE = BASE
CAPTURE = BASE.parent / "native-caller-map/source"
if not CAPTURE.exists():
    CAPTURE = SOURCE


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


def functions(path, names, namespace):
    tree = ast.parse(path.read_text(), filename=str(path))
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert {n.name for n in nodes} == set(names)
    prefix = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    tree = ast.fix_missing_locations(ast.Module(body=[prefix, *nodes], type_ignores=[]))
    exec(compile(tree, str(path), "exec"), namespace)
    return namespace


def _capture_defines(relative, names):
    candidate = CAPTURE / relative
    if not candidate.exists():
        return False
    tree = ast.parse(candidate.read_text(), filename=str(candidate))
    found = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    return set(names) <= found


# The segmented executor and batch planner exist only in the pinned capture
# (main-line tree); this branch's base predates them. Tests driving those real
# entry points run in the pinned harness and skip under repo CI.
CAPTURE_HAS_SEGMENTED_EXECUTOR = _capture_defines(
    "agent/tool_executor.py", ["execute_tool_calls_sequential", "execute_tool_calls_segmented"]) and _capture_defines(
    "agent/tool_dispatch_helpers.py", ["_plan_tool_batch_segments"])
SEGMENTED_UNAVAILABLE = "captured segmented executor unavailable on this base"


def package(name):
    value = types.ModuleType(name)
    value.__path__ = []
    return value


class CallerTests(unittest.TestCase):
    def setUp(self):
        self.modules = patch.dict(sys.modules)
        self.modules.start()
        self.addCleanup(self.modules.stop)
        for name in ["tools", "gateway", "agent", "hermes_cli", "acp_adapter"]:
            sys.modules[name] = package(name)
        self.registry_module = module("tools.registry", CAPTURE / "tools/registry.py")
        self.registry = self.registry_module.registry
        self.helper = module("gateway.context_tool", SOURCE / "gateway/context_tool.py")
        self.lock = threading.Lock()
        self.mcp = types.ModuleType("tools.mcp_tool")
        self.mcp._agent_tools_lock = self.lock
        self.mcp.sys = sys
        self.mcp.MCP_TOOL_NAME_PREFIX = "mcp__"
        functions(CAPTURE / "tools/mcp_tool.py", ["is_mcp_tool_parallel_safe"], self.mcp.__dict__)
        self.mcp._reinject_post_build_tools = lambda *a: set()
        functions(SOURCE / "tools/mcp_tool.py", ["refresh_agent_mcp_tools"], self.mcp.__dict__)
        sys.modules["tools.mcp_tool"] = self.mcp
        self.calls = []
        self.wall = [100.0]
        self.mono = [10.0]
        self.agent = types.SimpleNamespace(
            api_mode="chat_completions", tools=[{"type": "function", "function": {"name": "other"}}],
            valid_tool_names={"other"}, _tool_snapshot_generation=0,
            enabled_toolsets=None, disabled_toolsets=None,
            _context_engine_tool_names=set(), session_id="synthetic-session",
        )
        self.base_defs = [{"type": "function", "function": {"name": "fresh_other"}}]
        self.mt = types.ModuleType("model_tools")
        self.mt.get_tool_definitions = lambda **kw: list(self.base_defs)
        sys.modules["model_tools"] = self.mt

    def factory(self, *, agent):
        def invoke():
            self.calls.append(threading.current_thread())
            return types.SimpleNamespace(
                state="completed", reason="clean_context_pending_review", context_result={"approved": "B300 inventory"},
                closure_receipt={"cleanup_complete": True, "durable_capacity_released": False,
                                 "canonical_state": "execution_succeeded_pending_review"},
            )
        return self.helper.PreparedContextCall(
            invoke=invoke, validate=lambda: True, project_result=lambda result: {"status": "completed", "context": "B300 inventory"},
            expires_at=110.0, clock=lambda: self.wall[0], monotonic_clock=lambda: self.mono[0],
        )

    def binding(self, factory=None):
        return self.helper.bind_context_tool(self.agent, factory=factory or self.factory,
                                             registry=self.registry, snapshot_lock=self.lock)

    def names(self):
        return {item["function"]["name"] for item in self.agent.tools}

    def test_actual_refresh_preserves_scoped_tool_and_unrelated_new_tools(self):
        with self.binding():
            self.mcp.refresh_agent_mcp_tools(self.agent)
            self.assertIn(self.helper.TOOL_NAME, self.names())
            self.assertIn("fresh_other", self.names())
        self.assertNotIn(self.helper.TOOL_NAME, self.names())
        self.assertIn("fresh_other", self.names())

    def test_refresh_staged_before_close_cannot_resurrect_capability(self):
        scope = self.binding()
        scope.__enter__()
        stale = list(self.agent.tools)
        self.mt.get_tool_definitions = lambda **kw: (scope.__exit__(None, None, None), stale)[1]
        self.mcp.refresh_agent_mcp_tools(self.agent)
        self.assertNotIn(self.helper.TOOL_NAME, self.names())
        self.assertNotIn(self.helper.TOOL_NAME, self.agent.valid_tool_names)

    def test_actual_refresh_after_close_removes_owned_name_from_staged_catalog(self):
        with self.binding():
            stale = list(self.agent.tools)
        self.mt.get_tool_definitions = lambda **kw: stale
        self.mcp.refresh_agent_mcp_tools(self.agent)
        self.assertNotIn(self.helper.TOOL_NAME, self.names())

    def test_preexisting_foreign_collision_survives_actual_refresh(self):
        foreign = {"name": self.helper.TOOL_NAME, "description": "unrelated existing tool",
                   "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}}
        handler = lambda args, **kw: "foreign tool result"
        self.registry.register(self.helper.TOOL_NAME, "foreign-toolset", foreign, handler)
        entry = self.registry.get_entry(self.helper.TOOL_NAME)
        self.base_defs.append({"type": "function", "function": foreign})
        with self.binding():
            self.assertEqual(self.calls, [])
            self.mcp.refresh_agent_mcp_tools(self.agent)
            self.assertIn(self.helper.TOOL_NAME, self.agent.valid_tool_names)
            self.assertIn({"type": "function", "function": foreign}, self.agent.tools)
        self.assertIs(self.registry.get_entry(self.helper.TOOL_NAME), entry)

    def test_replaced_foreign_entry_survives_actual_refresh_without_cloud_invoke(self):
        foreign = {"name": self.helper.TOOL_NAME, "description": "replacement independent tool",
                   "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}}
        with self.binding():
            self.registry.deregister(self.helper.TOOL_NAME)
            self.registry.register(self.helper.TOOL_NAME, "foreign-toolset", foreign, lambda args, **kw: "foreign")
            self.base_defs.append({"type": "function", "function": foreign})
            self.mcp.refresh_agent_mcp_tools(self.agent)
            self.assertIn({"type": "function", "function": foreign}, self.agent.tools)
            self.assertEqual(self.calls, [])
        self.assertIn({"type": "function", "function": foreign}, self.agent.tools)

    def test_cached_agent_reuse_restores_one_scoped_schema(self):
        for _ in range(2):
            with self.binding():
                self.mcp.refresh_agent_mcp_tools(self.agent)
                self.assertEqual(sum(v["function"]["name"] == self.helper.TOOL_NAME for v in self.agent.tools), 1)
            self.assertNotIn(self.helper.TOOL_NAME, self.names())

    def _foreground(self):
        path = SOURCE / "gateway/run.py"
        tree = ast.parse(path.read_text())
        scopes = [n for n in ast.walk(tree) if isinstance(n, ast.With)
                  and any(isinstance(i.context_expr, ast.Call) and isinstance(i.context_expr.func, ast.Name)
                          and i.context_expr.func.id == "foreground_worker" for i in n.items)]
        self.assertEqual(len(scopes), 1)
        wrapper = ast.parse("def foreground(self, agent, _api_run_message, _conversation_kwargs):\n    pass\n").body[0]
        wrapper.body = [scopes[0], ast.Return(value=ast.Name(id="result", ctx=ast.Load()))]
        ns = {"foreground_worker": contextlib.nullcontext}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), str(path), "exec"), ns)
        return ns["foreground"]

    def test_actual_gateway_foreground_exposes_then_removes_tool(self):
        seen = []
        self.agent.run_conversation = lambda *a, **kw: seen.append(self.helper.TOOL_NAME in self.names()) or "reply"
        result = self._foreground()(types.SimpleNamespace(_context_tool_factory=self.factory), self.agent, "request", {})
        self.assertEqual(result, "reply")
        self.assertEqual(seen, [True])
        self.assertNotIn(self.helper.TOOL_NAME, self.names())

    def test_default_gateway_path_does_not_prepare_cloud(self):
        self.agent.run_conversation = lambda *a, **kw: "normal reply"
        result = self._foreground()(types.SimpleNamespace(_context_tool_factory=None), self.agent, "request", {})
        self.assertEqual(result, "normal reply")
        self.assertEqual(self.calls, [])
        self.assertNotIn(self.helper.TOOL_NAME, self.names())

    def test_actual_gateway_exception_closes_exposure(self):
        def fail(*a, **kw):
            raise ValueError("synthetic conversation failure")
        self.agent.run_conversation = fail
        with self.assertRaises(ValueError):
            self._foreground()(types.SimpleNamespace(_context_tool_factory=self.factory), self.agent, "request", {})
        self.assertNotIn(self.helper.TOOL_NAME, self.names())

    def _executor(self, quiet):
        noop = lambda *a, **kw: None
        plugins = types.ModuleType("hermes_cli.plugins")
        plugins.resolve_pre_tool_block = noop
        plugins.invoke_hook = lambda *a, **kw: []
        sys.modules[plugins.__name__] = plugins
        middleware = types.ModuleType("hermes_cli.middleware")
        middleware.run_tool_execution_middleware = lambda name, args, call, **kw: call(args)
        sys.modules[middleware.__name__] = middleware
        runtime = types.ModuleType("agent.agent_runtime_helpers")
        runtime.agent_runtime_owns_post_tool_hook = lambda *a: False
        sys.modules[runtime.__name__] = runtime
        self.mt.__dict__.update(json=json, time=time, logger=logging.getLogger("synthetic"), registry=self.registry,
                                coerce_tool_args=lambda name, args: args, _AGENT_LOOP_TOOLS=set(),
                                _READ_SEARCH_TOOLS=set(), _emit_post_tool_call_hook=noop,
                                _sanitize_tool_error=lambda value: "blocked", _apply_tool_result_transforms=lambda **kw: kw["result"])
        functions(CAPTURE / "model_tools.py", ["handle_function_call"], self.mt.__dict__)
        ns = dict(json=json, time=time, logging=logging, logger=logging.getLogger("synthetic"), Path=Path,
                  _ra=lambda: self.mt, _budget_for_agent=lambda a: {}, get_active_env=lambda tid: None,
                  _parse_tool_arguments=lambda v: (json.loads(v), None),
                  _apply_tool_request_middleware_for_agent=lambda a, **kw: (kw["function_args"], []),
                  _detect_tool_failure=lambda *a: (False, None), _is_multimodal_tool_result=lambda r: False,
                  _get_cute_tool_message_impl=lambda *a, **kw: "synthetic tool completion",
                  maybe_persist_tool_result=lambda **kw: kw["content"],
                  make_tool_result_message=lambda name, content, ident, **kw: {"role": "tool", "content": content, "tool_call_id": ident},
                  _flush_session_db_after_tool_progress=noop, enforce_turn_budget=noop)
        functions(CAPTURE / "agent/tool_executor.py", ["execute_tool_calls_sequential", "execute_tool_calls_segmented"], ns)
        ns["execute_tool_calls_concurrent"] = lambda agent, message, messages, tid, count, **kw: messages.extend(
            {"role": "tool", "tool_call_id": call.id, "content": "synthetic parallel read"} for call in message.tool_calls)
        for key, value in dict(quiet_mode=quiet, verbose_logging=False, tool_progress_mode="off", tool_delay=0,
                              _interrupt_requested=False, log_prefix="", _memory_manager=None,
                              tool_progress_callback=None, tool_start_callback=None, tool_complete_callback=None,
                              _should_emit_quiet_tool_messages=lambda: False, _should_start_quiet_spinner=lambda: False,
                              _tool_guardrails=types.SimpleNamespace(before_call=lambda *a: types.SimpleNamespace(allows_execution=True)),
                              _touch_activity=noop, _append_guardrail_observation=lambda n,a,r,**kw:r,
                              _record_file_mutation_result=noop, _apply_pending_steer_to_tool_results=noop,
                              _subdirectory_hints=types.SimpleNamespace(check_tool_call=lambda *a: None),
                              _tool_result_content_for_active_model=lambda n,r:r).items():
            setattr(self.agent, key, value)
        return ns

    @unittest.skipUnless(CAPTURE_HAS_SEGMENTED_EXECUTOR, SEGMENTED_UNAVAILABLE)
    def test_actual_sequential_registry_dispatch_quiet_and_nonquiet(self):
        for quiet in [True, False]:
            with self.subTest(quiet=quiet), self.binding():
                ns = self._executor(quiet)
                call = types.SimpleNamespace(id="call-1", function=types.SimpleNamespace(name=self.helper.TOOL_NAME, arguments="{}"))
                messages = []
                ns["execute_tool_calls_sequential"](self.agent, types.SimpleNamespace(tool_calls=[call]), messages, "task")
                self.assertEqual(messages[0]["tool_call_id"], "call-1")
                self.assertIn("B300 inventory", messages[0]["content"])
        self.assertEqual(self.calls, [threading.current_thread(), threading.current_thread()])

    @unittest.skipUnless(CAPTURE_HAS_SEGMENTED_EXECUTOR, SEGMENTED_UNAVAILABLE)
    def test_actual_mixed_planner_keeps_cloud_call_on_original_owner(self):
        ns = self._executor(True)
        ns.update(_NEVER_PARALLEL_TOOLS=frozenset({"clarify"}), _PARALLEL_SAFE_TOOLS=frozenset({"web_search"}),
                  _PATH_SCOPED_TOOLS=frozenset())
        functions(CAPTURE / "agent/tool_dispatch_helpers.py", ["_is_mcp_tool_parallel_safe", "_plan_tool_batch_segments"], ns)
        names = ["web_search", "web_search", self.helper.TOOL_NAME, "web_search", "web_search"]
        calls = [types.SimpleNamespace(id=str(i), function=types.SimpleNamespace(name=name, arguments="{}")) for i,name in enumerate(names)]
        segments = ns["_plan_tool_batch_segments"](calls)
        self.assertEqual([v[0] for v in segments], ["parallel", "sequential", "parallel"])
        with self.binding():
            messages = []
            ns["execute_tool_calls_segmented"](self.agent, types.SimpleNamespace(tool_calls=calls), messages, "task", segments=segments)
        self.assertEqual(self.calls, [threading.current_thread()])
        self.assertEqual([m["tool_call_id"] for m in messages], ["0", "1", "2", "3", "4"])
        self.assertIn("B300 inventory", messages[2]["content"])

    @unittest.skipUnless(CAPTURE_HAS_SEGMENTED_EXECUTOR, SEGMENTED_UNAVAILABLE)
    def test_actual_sequential_copied_context_cannot_invoke_provider(self):
        ns = self._executor(True)
        call = types.SimpleNamespace(id="copied", function=types.SimpleNamespace(name=self.helper.TOOL_NAME, arguments="{}"))
        message = types.SimpleNamespace(tool_calls=[call])
        with self.binding():
            messages = []
            contextvars.copy_context().run(ns["execute_tool_calls_sequential"], self.agent, message, messages, "task")
            self.assertEqual(json.loads(messages[0]["content"]), {"status": "held"})
            self.assertEqual(self.calls, [])
            original = []
            ns["execute_tool_calls_sequential"](self.agent, message, original, "task")
            self.assertEqual(json.loads(original[0]["content"])["status"], "completed")
        self.assertEqual(self.calls, [threading.current_thread()])

    @unittest.skipUnless(CAPTURE_HAS_SEGMENTED_EXECUTOR, SEGMENTED_UNAVAILABLE)
    def test_unknown_result_through_actual_dispatch_never_retries(self):
        ns = self._executor(False)
        def factory(**kw):
            prepared = self.factory(**kw)
            def unknown():
                self.calls.append("unknown")
                return types.SimpleNamespace(state="unknown", reason="PRIVATE")
            from dataclasses import replace
            return replace(prepared, invoke=unknown)
        call = types.SimpleNamespace(id="unknown", function=types.SimpleNamespace(name=self.helper.TOOL_NAME, arguments="{}"))
        with self.binding(factory):
            messages = []
            ns["execute_tool_calls_sequential"](self.agent, types.SimpleNamespace(tool_calls=[call, call]), messages, "task")
            self.assertEqual([json.loads(m["content"]) for m in messages], [{"status": "held"}, {"status": "held"}])
        self.assertEqual(self.calls, ["unknown"])


if __name__ == "__main__":
    unittest.main()
