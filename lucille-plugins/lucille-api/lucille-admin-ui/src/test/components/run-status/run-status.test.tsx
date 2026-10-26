import { describe, it, expect } from "vitest"
import { render, screen } from "@testing-library/react"
import type { Run } from "@/types/api"
import { runStatus, RunStatusBadge } from "@/components/run-status/run-status"

function run(overrides: Partial<Run>): Run {
  return {
    runId: "r1",
    configId: "c1",
    startTime: null,
    endTime: null,
    runResult: null,
    runType: null,
    done: false,
    ...overrides,
  }
}

describe("runStatus", () => {
  it("is Running when the run is not done", () => {
    expect(runStatus(run({ done: false }))).toEqual({ label: "Running", tone: "running" })
  })

  it("is Succeeded when done with a successful result", () => {
    const r = run({ done: true, runResult: { status: true, message: "ok", runId: "r1" } })
    expect(runStatus(r)).toEqual({ label: "Succeeded", tone: "success" })
  })

  it("is Failed when done with a failed result", () => {
    const r = run({ done: true, runResult: { status: false, message: "err", runId: "r1" } })
    expect(runStatus(r)).toEqual({ label: "Failed", tone: "failure" })
  })

  it("is Failed when done with no result", () => {
    expect(runStatus(run({ done: true, runResult: null }))).toEqual({
      label: "Failed",
      tone: "failure",
    })
  })
})

describe("RunStatusBadge", () => {
  it("renders the status label", () => {
    render(<RunStatusBadge run={run({ done: false })} />)
    expect(screen.getByText("Running")).toBeInTheDocument()
  })

  it("renders Succeeded for a successful completed run", () => {
    render(<RunStatusBadge run={run({ done: true, runResult: { status: true, message: "", runId: "r1" } })} />)
    expect(screen.getByText("Succeeded")).toBeInTheDocument()
  })
})
