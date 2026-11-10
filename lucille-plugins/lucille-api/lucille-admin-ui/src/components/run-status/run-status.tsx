import type { Run } from "@/types/api"
import type { RunStatus, RunTone } from "./run-status.types"
import styles from "./run-status.module.css"

// Running until done; then Succeeded/Failed from runResult.status.
export function runStatus(run: Run): RunStatus {
  if (!run.done) {
    return { label: "Running", tone: "running" }
  }
  if (run.runResult) {
    return run.runResult.status
      ? { label: "Succeeded", tone: "success" }
      : { label: "Failed", tone: "failure" }
  }
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
