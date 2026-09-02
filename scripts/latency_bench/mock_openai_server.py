#!/usr/bin/env python3
"""Minimal OpenAI-compatible mock model server for latency benchmarks.

Serves /v1/models, /v1/chat/completions (streaming and non-streaming) and
/v1/responses (streaming) with a canned short text reply. Latency is
configurable so the hermes overhead can be isolated from the model:

  --ttft-ms   delay before the first token is emitted (default 0)
  --tps       tokens per second while streaming (default 0 = instant)

Every request is appended as one JSON line to --log so the benchmark can
report the request shape hermes actually sent (message count, tool count,
approximate prompt bytes).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid

from aiohttp import web

REPLY = "ok, noted."


def _approx_tokens(payload: dict) -> int:
    return len(json.dumps(payload)) // 4


async def _log_request(app, path: str, payload: dict) -> None:
    rec = {
        "ts": time.time(),
        "path": path,
        "stream": bool(payload.get("stream")),
        "model": payload.get("model"),
        "messages": len(payload.get("messages") or payload.get("input") or []),
        "tools": len(payload.get("tools") or []),
        "prompt_bytes": len(json.dumps(payload)),
        "approx_tokens": _approx_tokens(payload),
        "has_instructions": bool(payload.get("instructions")),
        "reasoning": payload.get("reasoning") or payload.get("reasoning_effort"),
    }
    log = app["log"]
    if log:
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")


async def control(request):
    """Arm behaviours for upcoming requests: {"stall_next": N} makes the next N
    model requests hang without sending a byte (simulates a wedged stream)."""
    payload = await request.json()
    request.app["stall_next"] = int(payload.get("stall_next", 0))
    return web.json_response({"ok": True, "stall_next": request.app["stall_next"]})


async def _maybe_stall(app, request):
    if app.get("stall_next", 0) > 0:
        app["stall_next"] -= 1
        # Keep the connection open and silent until the client goes away.
        try:
            await asyncio.sleep(app["stall_seconds"])
        except asyncio.CancelledError:
            raise
        raise web.HTTPServiceUnavailable(text="stalled")


async def models(request):
    return web.json_response({"object": "list", "data": [{"id": "bench-model", "object": "model"}]})


async def chat_completions(request):
    app = request.app
    payload = await request.json()
    await _log_request(app, "/v1/chat/completions", payload)
    await _maybe_stall(app, request)
    text = app["reply"]
    words = text.split(" ")
    cid = f"chatcmpl-{uuid.uuid4().hex[:12]}"
    created = int(time.time())
    usage = {"prompt_tokens": _approx_tokens(payload), "completion_tokens": len(words), "total_tokens": _approx_tokens(payload) + len(words)}
    if app["ttft"]:
        await asyncio.sleep(app["ttft"])
    if not payload.get("stream"):
        return web.json_response({
            "id": cid, "object": "chat.completion", "created": created, "model": payload.get("model"),
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": usage,
        })
    resp = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
    await resp.prepare(request)

    def chunk(delta, finish=None, extra=None):
        body = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": payload.get("model"),
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
        if extra:
            body.update(extra)
        return f"data: {json.dumps(body)}\n\n".encode()

    await resp.write(chunk({"role": "assistant", "content": ""}))
    for i, w in enumerate(words):
        await resp.write(chunk({"content": (" " if i else "") + w}))
        if app["tps"]:
            await asyncio.sleep(1.0 / app["tps"])
    await resp.write(chunk({}, finish="stop"))
    await resp.write(chunk({}, extra={"usage": usage}))
    await resp.write(b"data: [DONE]\n\n")
    await resp.write_eof()
    return resp


async def responses(request):
    app = request.app
    payload = await request.json()
    await _log_request(app, "/v1/responses", payload)
    await _maybe_stall(app, request)
    text = app["reply"]
    rid = f"resp_{uuid.uuid4().hex[:16]}"
    mid = f"msg_{uuid.uuid4().hex[:16]}"
    usage = {"input_tokens": _approx_tokens(payload), "output_tokens": len(text.split()), "total_tokens": _approx_tokens(payload) + len(text.split())}
    final = {"id": rid, "object": "response", "status": "completed", "model": payload.get("model"),
             "output": [{"id": mid, "type": "message", "role": "assistant", "status": "completed",
                         "content": [{"type": "output_text", "text": text, "annotations": []}]}],
             "usage": usage}
    if app["ttft"]:
        await asyncio.sleep(app["ttft"])
    if not payload.get("stream"):
        return web.json_response(final)
    resp = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
    await resp.prepare(request)

    async def ev(kind, body):
        await resp.write(f"event: {kind}\ndata: {json.dumps({'type': kind, **body})}\n\n".encode())

    await ev("response.created", {"response": {**final, "status": "in_progress", "output": []}})
    await ev("response.output_item.added", {"output_index": 0, "item": {"id": mid, "type": "message", "role": "assistant", "status": "in_progress", "content": []}})
    for i, w in enumerate(text.split(" ")):
        await ev("response.output_text.delta", {"item_id": mid, "output_index": 0, "content_index": 0, "delta": (" " if i else "") + w})
        if app["tps"]:
            await asyncio.sleep(1.0 / app["tps"])
    await ev("response.output_text.done", {"item_id": mid, "output_index": 0, "content_index": 0, "text": text})
    await ev("response.output_item.done", {"output_index": 0, "item": final["output"][0]})
    await ev("response.completed", {"response": final})
    await resp.write_eof()
    return resp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18642)
    ap.add_argument("--ttft-ms", type=float, default=0.0)
    ap.add_argument("--tps", type=float, default=0.0)
    ap.add_argument("--reply", default=REPLY)
    ap.add_argument("--log", default="")
    ap.add_argument("--stall-seconds", type=float, default=600.0)
    args = ap.parse_args()
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app["ttft"] = args.ttft_ms / 1000.0
    app["tps"] = args.tps
    app["reply"] = args.reply
    app["log"] = args.log
    app["stall_next"] = 0
    app["stall_seconds"] = args.stall_seconds
    app.router.add_get("/v1/models", models)
    app.router.add_post("/__control", control)
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_post("/v1/responses", responses)
    web.run_app(app, host="127.0.0.1", port=args.port, print=lambda *_: None)


if __name__ == "__main__":
    main()
