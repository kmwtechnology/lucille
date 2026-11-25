// Tests the Runs page: per-run document counts and fractional-second duration.
import { describe, it, expect } from "vitest"
import { screen, within } from "@testing-library/react"
import Runs from "@/pages/runs/runs"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("Runs page", () => {
  // A finished run shows its succeeded/failed/dropped counts and a fractional-second duration.
  it("sums document counts across connectors and shows precise duration for a finished run", async () => {
    mockFetch({
      "/v1/run": {
        json: [
          {
            runId: "run-1",
            configId: "cfg-1",
            startTime: 1_700_000_000,
            endTime: 1_700_000_012.345,
            runResult: {
              status: true,
              connectorResults: [
                { name: "c1", status: true, numSucceeded: 5, numFailed: 0, numDropped: 1 },
                { name: "c2", status: true, numSucceeded: 3, numFailed: 2, numDropped: 4 },
              ],
              message: "",
              runId: "run-1",
            },
            runType: "LOCAL",
            done: true,
          },
        ],
      },
    })
    renderWithRouter(<Runs />)
    const row = (await screen.findByText("run-1")).closest("tr") as HTMLElement
    const cells = within(row).getAllByRole("cell")
    expect(cells.slice(4).map((c) => c.textContent)).toEqual(["8", "2", "5", "12.345s"])
  })

  // A run still in progress has no counts yet and no final duration.
  it("shows placeholders for an in-progress run", async () => {
    mockFetch({
      "/v1/run": {
        json: [
          {
            runId: "run-2",
            configId: "cfg-1",
            startTime: 1_700_000_000,
            endTime: null,
            runResult: null,
            runType: null,
            done: false,
          },
        ],
      },
    })
    renderWithRouter(<Runs />)
    const row = (await screen.findByText("run-2")).closest("tr") as HTMLElement
    const cells = within(row).getAllByRole("cell")
    expect(cells.slice(4).map((c) => c.textContent)).toEqual(["—", "—", "—", "In progress"])
  })
})
