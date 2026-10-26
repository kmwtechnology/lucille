import { describe, it, expect } from "vitest"
import { render, screen } from "@testing-library/react"
import type { ConfigObject } from "@/types/api"
import { ConfigChips, indexerLabel, shortClassName } from "@/components/config-summary/config-summary"

describe("shortClassName", () => {
  it("returns the last segment of a fully-qualified class name", () => {
    expect(shortClassName("com.kmwllc.lucille.indexer.SolrIndexer")).toBe("SolrIndexer")
  })

  it("returns the input unchanged when there is no dot", () => {
    expect(shortClassName("SolrIndexer")).toBe("SolrIndexer")
  })

  it("returns an empty string for undefined", () => {
    expect(shortClassName(undefined)).toBe("")
  })
})

describe("indexerLabel", () => {
  it("returns None when there is no indexer", () => {
    expect(indexerLabel({})).toBe("None")
  })

  it("prefers the short class name when class is set", () => {
    const config: ConfigObject = { indexer: { class: "com.kmwllc.lucille.indexer.CSVIndexer" } }
    expect(indexerLabel(config)).toBe("CSVIndexer")
  })

  it("falls back to type when class is absent", () => {
    const config: ConfigObject = { indexer: { type: "CSV" } }
    expect(indexerLabel(config)).toBe("CSV")
  })

  it("returns Custom when an indexer exists but has neither class nor type", () => {
    const config: ConfigObject = { indexer: { somethingElse: true } }
    expect(indexerLabel(config)).toBe("Custom")
  })
})

describe("ConfigChips", () => {
  it("renders singular labels for a single connector and pipeline", () => {
    const config: ConfigObject = {
      connectors: [{ class: "CSVConnector" }],
      pipelines: [{ name: "p1" }],
      indexer: { type: "CSV" },
    }

    render(<ConfigChips config={config} />)

    expect(screen.getByText("1 Connector")).toBeInTheDocument()
    expect(screen.getByText("1 Pipeline")).toBeInTheDocument()
    expect(screen.getByText("CSV")).toBeInTheDocument()
  })

  it("renders plural labels and zero counts when collections are empty or missing", () => {
    const config: ConfigObject = {
      connectors: [{}, {}],
      pipelines: [],
    }

    render(<ConfigChips config={config} />)

    expect(screen.getByText("2 Connectors")).toBeInTheDocument()
    expect(screen.getByText("0 Pipelines")).toBeInTheDocument()
    expect(screen.getByText("None")).toBeInTheDocument()
  })
})
