import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import ConfigDetail from "@/pages/config-detail/config-detail"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

function renderAt(id: string) {
  return renderWithRouter(<ConfigDetail />, {
    path: "/configs/:id",
    route: `/configs/${id}`,
  })
}

describe("ConfigDetail page", () => {
  it("shows the error state when the config cannot be loaded", async () => {
    mockFetchReject()

    renderAt("missing")

    expect(await screen.findByText("Configuration not found")).toBeInTheDocument()
  })

  it("renders the config id, chips, and section cards on success", async () => {
    mockFetch({
      "/v1/config/": {
        json: {
          connectors: [{ name: "conn1", class: "com.kmwllc.lucille.connector.CSVConnector", pipeline: "p1" }],
          pipelines: [{ name: "p1", stages: [] }],
          indexer: { type: "CSV" },
        },
      },
    })

    renderAt("abc-123")

    expect(await screen.findByText("abc-123")).toBeInTheDocument()
    expect(screen.getByText(/Connectors \(1\)/)).toBeInTheDocument()
    expect(screen.getByText(/Pipelines \(1\)/)).toBeInTheDocument()
    expect(screen.getByText("Indexer")).toBeInTheDocument()
  })
})
