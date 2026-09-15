import { useContext, useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { StatusResponse } from "@/lib/api";
import { ProfileContext } from "@/contexts/profile-context";
import { createSidebarStatusPoll } from "@/lib/sidebar-status-poll";

const POLL_MS = 10_000;

/** Response-bound, profile-specific status for the app shell. */
export function useSidebarStatus() {
  const { profile } = useContext(ProfileContext);
  const [snapshot, setSnapshot] = useState<{
    profile: string;
    status: StatusResponse | null;
  } | null>(null);

  useEffect(() => {
    const poll = createSidebarStatusPoll(
      () => api.getStatus(profile),
      (status) => setSnapshot({ profile, status }),
    );
    void poll.load();
    const id = setInterval(() => void poll.load(), POLL_MS);
    return () => {
      poll.close();
      clearInterval(id);
    };
  }, [profile]);

  // Hide the old scope immediately, before the replacement effect settles.
  return snapshot?.profile === profile ? snapshot.status : null;
}
