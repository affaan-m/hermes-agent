import type { StatusResponse } from "./api";

/** One profile-effect lifetime. Coalesce polls and ignore late responses. */
export function createSidebarStatusPoll(
  read: () => Promise<StatusResponse>,
  publish: (status: StatusResponse | null) => void,
) {
  let closed = false;
  let loading = false;
  return {
    async load() {
      if (closed || loading) return;
      loading = true;
      try {
        const value = await read();
        if (!closed) publish(value);
      } catch {
        if (!closed) publish(null);
      } finally {
        loading = false;
      }
    },
    close() {
      closed = true;
    },
  };
}
