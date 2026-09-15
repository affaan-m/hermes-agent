import { Link } from "react-router";
import type { StatusResponse } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useI18n } from "@/i18n";
import { en } from "@/i18n/en";

/** Gateway + session summary for the System sidebar block (no separate strip chrome). */
export function SidebarStatusStrip({ status }: SidebarStatusStripProps) {
  const { t } = useI18n();
  const labels = t.app.sidebarStatus ?? en.app.sidebarStatus!;

  if (status === null) {
    return (
      <p className="px-5 py-1.5 text-xs text-text-tertiary" role="status">
        {labels.unavailable}
      </p>
    );
  }

  const gw = gatewayLine(status, t);
  const { gatewayStatusLabel } = t.app;
  const sessionsAvailable = status.active_sessions_available === true
    && typeof status.active_sessions === "number" && Number.isInteger(status.active_sessions)
    && status.active_sessions >= 0;
  const windowLabel = status.active_sessions_window_seconds === undefined
    ? t.common.unknown
    : labels.recentWindow.replace("{seconds}", String(status.active_sessions_window_seconds));
  const sampleLabel = status.active_sessions_limit === undefined
    ? t.common.unknown
    : labels.sampleLimit.replace("{limit}", String(status.active_sessions_limit));

  return (
    <Link
      to="/sessions"
      title={t.app.statusOverview}
      className={cn(
        "block text-left",
        "px-5 pb-2 pt-0.5",
        "text-text-secondary",
        "transition-colors hover:text-midground",
        "focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-midground/40",
        "focus-visible:ring-inset",
      )}
    >
      <div className="flex flex-col gap-1 font-sans text-xs leading-snug tracking-[0.08em]">
        <p className="break-words text-text-tertiary">
          {labels.profile}: {status.gateway_profile ?? t.common.unknown}
        </p>
        <p className="break-words">
          <span className="text-text-tertiary">{gatewayStatusLabel}</span>{" "}
          <span className={cn("font-medium", gw.tone)}>{gw.label}</span>
        </p>

        <p className="break-words" title={sampleLabel}>
          <span className="text-text-tertiary">{t.status.recentSessions} ({windowLabel}):</span>{" "}
          <span className="tabular-nums text-text-secondary">
            {sessionsAvailable ? status.active_sessions : t.common.unknown}
          </span>
        </p>
        <p className="text-text-tertiary">{sampleLabel}</p>
      </div>
    </Link>
  );
}

export function gatewayLine(
  status: StatusResponse,
  t: ReturnType<typeof useI18n>["t"],
): { label: string; tone: string } {
  const g = t.app.gatewayStrip;
  const byState: Record<string, { label: string; tone: string }> = {
    running: { label: g.running, tone: "text-success" },
    starting: { label: g.starting, tone: "text-warning" },
    startup_failed: { label: g.failed, tone: "text-destructive" },
    stopped: { label: g.stopped, tone: "text-muted-foreground" },
  };
  if (status.gateway_state && byState[status.gateway_state]) {
    return byState[status.gateway_state];
  }
  return status.gateway_running
    ? { label: g.running, tone: "text-success" }
    : { label: g.off, tone: "text-muted-foreground" };
}

interface SidebarStatusStripProps {
  status: StatusResponse | null;
}
