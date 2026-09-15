// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, setManagementProfile } from "./api";

beforeEach(() => {
  setManagementProfile("ito");
  Object.defineProperty(window, "__HERMES_SESSION_TOKEN__", { configurable: true, value: "synthetic-status-session" });
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ gateway_profile: "ito" }), { status: 200, headers: { "Content-Type": "application/json" } })));
});
afterEach(() => {
  setManagementProfile("");
  Object.defineProperty(window, "__HERMES_SESSION_TOKEN__", { configurable: true, value: undefined });
  vi.unstubAllGlobals();
});

describe("status API scope", () => {
  it("uses management scope for legacy calls but pins explicit dashboard scope", async () => {
    await api.getStatus();
    await api.getStatus("");
    expect(vi.mocked(fetch).mock.calls[0][0]).toBe("/api/status?profile=ito");
    expect(vi.mocked(fetch).mock.calls[1][0]).toBe("/api/status?profile=current");
  });
  it("encodes explicit profiles once and preserves existing session and cookie auth", async () => {
    await api.getStatus("other profile");
    const [url, options] = vi.mocked(fetch).mock.calls[0];
    expect(url).toBe("/api/status?profile=other%20profile");
    expect(new Headers(options?.headers).get("X-Hermes-Session-Token")).toBe("synthetic-status-session");
    expect(options?.credentials).toBe("include");
  });
});
