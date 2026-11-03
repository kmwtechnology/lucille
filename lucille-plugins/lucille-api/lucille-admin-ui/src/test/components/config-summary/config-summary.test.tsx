// Tests the config-summary helpers and chips: class-name shortening, indexer labels, and connector/pipeline counts.
import { describe, it, expect } from "vitest"
import { render, screen } from "@testing-library/react"
import type { ConfigObject } from "@/types/api"
import { ConfigChips, indexerLabel, shortClassName } from "@/components/config-summary/config-summary"

describe("shortClassName", () => {
  // A fully-qualified name is shortened to just the class for display.
  it("returns the last segment of a fully-qualified class name", () => {
    expect(shortClassName("com.kmwllc.lucille.indexer.SolrIndexer")).toBe("SolrIndexer")
  })

  // An already-short name (no package) is returned as-is.
  it("returns the input unchanged when there is no dot", () => {
    expect(shortClassName("SolrIndexer")).toBe("SolrIndexer")
  })

  // A missing class name yields an empty string rather than throwing.
  it("returns an empty string for undefined", () => {
    expect(shortClassName(undefined)).toBe("")
  })
})

describe("indexerLabel", () => {
  // A config with no indexer shows "None".
  it("returns None when there is no indexer", () => {
    expect(indexerLabel({})).toBe("None")
  })

  // When an indexer class is set, its short name is used as the label.
  it("prefers the short class name when class is set", () => {
    const config: ConfigObject = { indexer: { class: "com.kmwllc.lucille.indexer.CSVIndexer" } }
    expect(indexerLabel(config)).toBe("CSVIndexer")
  })

  // With no class, the indexer `type` is used instead.
  it("falls back to type when class is absent", () => {
    const config: ConfigObject = { indexer: { type: "CSV" } }
    expect(indexerLabel(config)).toBe("CSV")
  })

  // An indexer with neither class nor type is labelled "Custom".
  it("returns Custom when an indexer exists but has neither class nor type", () => {
    const config: ConfigObject = { indexer: { somethingElse: true } }
    expect(indexerLabel(config)).toBe("Custom")
  })
})

describe("ConfigChips", () => {
  // One connector/pipeline renders singular labels.
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

  // Multiple/empty collections render plural labels and a zero count.
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
