/**
 * Outcome of a completed run (RunResult). `status` true means success;
 * `message` is the human-readable run summary.
 */
export interface RunResult {
  status: boolean
  message: string
  runId: string
}

export interface Run {
  runId: string
  configId: string
  startTime: string | number | null
  endTime: string | number | null
  runResult: RunResult | null
  runType: string | null
  done: boolean
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
