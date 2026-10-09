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
 * A Lucille run (RunDetails). Times are ISO-8601 strings. `endTime` and
 * `runResult` are null until the run finishes. `hasThrowable` is true when the
 * run errored.
 */
export interface Run {
  runId: string
  configId: string
  startTime: string | null
  endTime: string | null
  runResult: RunResult | null
  runType: string | null
  done: boolean
  hasThrowable?: boolean
}

export interface SystemStats {
  cpu: { percent: number; used: number; available: number; total: number; loadAverage: number }
  ram: { total: number; available: number; used: number; percent: number }
  jvm: { total: number; free: number; used: number; percent: number }
  storage: { total: number; available: number; used: number; percent: number }
}

/**
 * A single Lucille configuration object. Contents are dynamic HOCON/JSON,
 * so most fields are loosely typed. The optional top-level keys below are the
 * ones commonly present and used for building list summaries.
 */
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
