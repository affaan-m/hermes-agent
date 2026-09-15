import { describe, expect, it, vi } from "vitest";
import type { StatusResponse } from "./api";
import { createSidebarStatusPoll } from "./sidebar-status-poll";

function deferred() {
  let resolve!: (value: StatusResponse) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<StatusResponse>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const status = { gateway_profile: "ito", gateway_running: true } as StatusResponse;

describe("profile status poll", () => {
  it("coalesces interval ticks while one request is pending and allows the next read", async () => {
    const pending = deferred();
    const read = vi.fn().mockReturnValueOnce(pending.promise).mockResolvedValue(status);
    const publish = vi.fn();
    const poll = createSidebarStatusPoll(read, publish);
    const first = poll.load();
    await poll.load();
    await poll.load();
    expect(read).toHaveBeenCalledTimes(1);
    pending.resolve(status);
    await first;
    expect(publish).toHaveBeenLastCalledWith(status);
    await poll.load();
    expect(read).toHaveBeenCalledTimes(2);
  });

  it("clears previous successful status on error and can recover", async () => {
    const read = vi.fn().mockResolvedValueOnce(status).mockRejectedValueOnce(new Error("offline")).mockResolvedValue(status);
    const publish = vi.fn();
    const poll = createSidebarStatusPoll(read, publish);
    await poll.load();
    await poll.load();
    expect(publish).toHaveBeenLastCalledWith(null);
    await poll.load();
    expect(publish).toHaveBeenLastCalledWith(status);
  });

  it.each(["success", "error"])("suppresses late %s after profile lifetime closes", async (outcome) => {
    const pending = deferred();
    const read = vi.fn(() => pending.promise);
    const publish = vi.fn();
    const poll = createSidebarStatusPoll(read, publish);
    const first = poll.load();
    poll.close();
    if (outcome === "success") pending.resolve(status); else pending.reject(new Error("old profile"));
    await first;
    await poll.load();
    expect(publish).not.toHaveBeenCalled();
    expect(read).toHaveBeenCalledTimes(1);
  });
});
