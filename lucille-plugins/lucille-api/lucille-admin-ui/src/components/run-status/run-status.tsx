import type { Run } from "@/types/api"
import styles from "./run-status.module.css"

export type RunTone = "running" | "success" | "failure"

export interface RunStatus {
  label: string
  tone: RunTone
}

/**
 * Derive a display status from a run. A run that isn't done is "Running".
 * Once done, success is determined by runResult.status, and an error
 * (hasThrowable, or a completed run with no successful result) is "Failed".
 */
export function runStatus(run: Run): RunStatus {
  if (!run.done) {
    return { label: "Running", tone: "running" }
  }
  if (run.hasThrowable) {
    return { label: "Failed", tone: "failure" }
  }
  if (run.runResult) {
    return run.runResult.status
      ? { label: "Succeeded", tone: "success" }
      : { label: "Failed", tone: "failure" }
  }
  // Done but no result recorded — treat as failure to be safe.
  return { label: "Failed", tone: "failure" }
}

const toneClass: Record<RunTone, string | undefined> = {
  running: styles.running,
  success: styles.success,
  failure: styles.failure,
}

export function RunStatusBadge({ run }: { run: Run }) {
  const status = runStatus(run)
  return <span className={`${styles.badge} ${toneClass[status.tone]}`}>{status.label}</span>
}
