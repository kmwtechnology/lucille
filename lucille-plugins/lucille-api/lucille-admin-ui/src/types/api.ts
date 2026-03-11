export interface Run {
  runId: string
  configId: string
  startTime: number | null
  endTime: number | null
  runResult: unknown
  runType: string
  done: boolean
  future?: {
    done?: boolean
    cancelled?: boolean
    completedExceptionally?: boolean
    numberOfDependents?: number
  }
}

export interface SystemStats {
  cpu: { percent: number; used: number; available: number; total: number; loadAverage: number }
  ram: { total: number; available: number; used: number; percent: number }
  jvm: { total: number; free: number; used: number; percent: number }
  storage: { total: number; available: number; used: number; percent: number }
}
