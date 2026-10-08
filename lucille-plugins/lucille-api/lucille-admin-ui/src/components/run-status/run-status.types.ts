export type RunTone = "running" | "success" | "failure"

export interface RunStatus {
  label: string
  tone: RunTone
}
