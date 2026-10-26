import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import Configs from "@/pages/configs/configs"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

describe("Configs page", () => {
  it("shows the empty state when there are no configs", async () => {
    mockFetch({ "/v1/config": { json: {} } })

    renderWithRouter(<Configs />)

    expect(await screen.findByText("No configurations yet")).toBeInTheDocument()
  })

  it("shows the error state when the fetch fails", async () => {
    mockFetchReject()

    renderWithRouter(<Configs />)

    expect(await screen.findByText("An error has occurred")).toBeInTheDocument()
    expect(screen.getByText("Unable to load configuration data")).toBeInTheDocument()
  })

  it("renders a row per config and the pagination summary on success", async () => {
    mockFetch({
      "/v1/config": {
        json: {
          "cfg-b": { connectors: [{ name: "c1" }], pipelines: [{ name: "p1" }], indexer: { type: "CSV" } },
          "cfg-a": { connectors: [], pipelines: [], indexer: { class: "x.y.SolrIndexer" } },
        },
      },
    })

    renderWithRouter(<Configs />)

    expect(await screen.findByText("cfg-a")).toBeInTheDocument()
    expect(screen.getByText("cfg-b")).toBeInTheDocument()
    expect(screen.getByText("SolrIndexer")).toBeInTheDocument()
    expect(screen.getByText("1–2 of 2 configs")).toBeInTheDocument()
  })
})
