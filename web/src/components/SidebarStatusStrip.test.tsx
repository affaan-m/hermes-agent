// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { I18nProvider } from "@/i18n";
import type { StatusResponse } from "@/lib/api";
import { SidebarStatusStrip } from "./SidebarStatusStrip";

function status(overrides: Partial<StatusResponse> = {}): StatusResponse {
  return { gateway_profile: "ito", gateway_running: true, gateway_state: "running",
    active_sessions: 0, active_sessions_available: true, active_sessions_window_seconds: 300,
    active_sessions_limit: 50, ...overrides } as StatusResponse;
}
function rendered(value: StatusResponse | null) {
  const html = renderToStaticMarkup(<MemoryRouter><I18nProvider><SidebarStatusStrip status={value} /></I18nProvider></MemoryRouter>);
  const node = document.createElement("div");
  node.innerHTML = html;
  return node;
}
beforeEach(() => {
  // Per-file jsdom does not guarantee ambient localStorage in every Node runtime.
  const store = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => { store.set(key, String(value)); },
    removeItem: (key: string) => { store.delete(key); },
    clear: () => { store.clear(); },
  });
});
afterEach(() => vi.unstubAllGlobals());

describe("sidebar response-bound labels", () => {
  it("renders the response profile and the bounded 300-second/50-conversation count", () => {
    const node = rendered(status());
    expect(node.textContent).toContain("Profile: ito");
    expect(node.textContent).toContain("last 300s");
    expect(node.textContent).toContain("sample of up to 50 newest conversations");
    expect(node.querySelector(".tabular-nums")?.textContent).toBe("0");
    expect(node.textContent).toContain("Running");
    expect(node.querySelector("a")?.getAttribute("href")).toBe("/sessions");
  });

  it("renders unavailable accessibly without presenting a stopped gateway or zero", () => {
    const node = rendered(null);
    expect(node.querySelector('[role="status"]')?.textContent).toBe("Status unavailable or loading");
    expect(node.querySelector(".tabular-nums")).toBeNull();
    expect(node.textContent).not.toContain("Stopped");
  });

  it.each([
    { active_sessions_available: false, active_sessions: 0 },
    { active_sessions_available: true, active_sessions: null },
    { active_sessions_available: true, active_sessions: -1 },
    { active_sessions_available: undefined, active_sessions: 0 },
  ])("keeps unavailable or invalid recent counts unknown: %j", (overrides) => {
    const node = rendered(status(overrides));
    expect(node.querySelector(".tabular-nums")?.textContent).toBe("unknown");
  });

  it("does not invent profile or sampling metadata for an older response", () => {
    const node = rendered(status({ gateway_profile: undefined, active_sessions_window_seconds: undefined, active_sessions_limit: undefined }));
    expect(node.textContent).toContain("Profile: unknown");
    expect(node.textContent).not.toContain("last 300s");
    expect(node.textContent).not.toContain("up to 50");
  });

  it("renders a source-provided profile name as text", () => {
    const node = rendered(status({ gateway_profile: '<img src=x onerror="alert(1)">' }));
    expect(node.querySelector("img")).toBeNull();
    expect(node.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});
