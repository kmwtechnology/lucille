/**
 * Outcome of a completed run (RunResult). `status` true means success;
 * `message` is the human-readable run summary.
 */
export interface RunResult {
  status: boolean
  message: string
  runId: string
}

/**
 * Why a run failed before producing a RunResult (e.g. a connector couldn't be
 * constructed). The root cause fields name the innermost cause, and repeat
 * className/message when there is none.
 */
export interface RunError {
  className: string
  message: string | null
  rootCauseClassName: string
  rootCauseMessage: string | null
}

export interface Run {
  runId: string
  configId: string
  startTime: string | number | null
  endTime: string | number | null
  runResult: RunResult | null
  runType: string | null
  done: boolean
  error: RunError | null
}

export interface SystemStats {
  cpu: { percent: number; used: number; available: number; total: number; loadAverage: number }
  ram: { total: number; available: number; used: number; percent: number }
  jvm: { total: number; free: number; used: number; percent: number }
  storage: { total: number; available: number; used: number; percent: number }
}

export interface ConfigObject {
  connectors?: Array<Record<string, unknown>>
  pipelines?: Array<{ name?: string; stages?: Array<Record<string, unknown>> }>
  indexer?: { type?: string; class?: string; [key: string]: unknown }
  [key: string]: unknown
}

/**
 * Response shape of GET /v1/config: a map keyed by config UUID.
 */
export type ConfigMap = Record<string, ConfigObject>
