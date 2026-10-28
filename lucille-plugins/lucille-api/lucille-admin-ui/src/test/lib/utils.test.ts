import { describe, it, expect } from "vitest"
import {
  cn,
  errorMessageFrom,
  formatBytes,
  formatDuration,
  formatInstant,
  toEpochMs,
} from "@/lib/utils"

describe("cn", () => {
  it("joins class names", () => {
    expect(cn("a", "b")).toBe("a b")
  })

  it("drops falsy values", () => {
    expect(cn("a", false, undefined, null, "b")).toBe("a b")
  })

  it("merges conflicting tailwind classes, last one wins", () => {
    expect(cn("px-2", "px-4")).toBe("px-4")
  })
})

describe("toEpochMs", () => {
  it("returns NaN for null, undefined, and empty string", () => {
    expect(toEpochMs(null)).toBeNaN()
    expect(toEpochMs(undefined)).toBeNaN()
    expect(toEpochMs("")).toBeNaN()
  })

  it("treats a number as epoch seconds and converts to ms", () => {
    expect(toEpochMs(1_700_000_000)).toBe(1_700_000_000_000)
  })

  it("treats a numeric string as epoch seconds", () => {
    expect(toEpochMs("1700000000")).toBe(1_700_000_000_000)
  })

  it("parses an ISO date string as a date", () => {
    expect(toEpochMs("2024-01-01T00:00:00.000Z")).toBe(Date.parse("2024-01-01T00:00:00.000Z"))
  })

  it("returns NaN for an unparseable string", () => {
    expect(toEpochMs("not-a-date")).toBeNaN()
  })
})

describe("formatInstant", () => {
  it("returns a dash when the value is absent", () => {
    expect(formatInstant(null)).toBe("—")
    expect(formatInstant(undefined)).toBe("—")
    expect(formatInstant("")).toBe("—")
  })

  it("formats a valid epoch-seconds number to a locale string", () => {
    const expected = new Date(1_700_000_000_000).toLocaleString()
    expect(formatInstant(1_700_000_000)).toBe(expected)
  })

  it("falls back to the raw string when unparseable", () => {
    expect(formatInstant("garbage")).toBe("garbage")
  })
})

describe("formatDuration", () => {
  it("returns a dash when start is absent", () => {
    expect(formatDuration(null, null)).toBe("—")
    expect(formatDuration("", 123)).toBe("—")
  })

  it("formats sub-minute durations in seconds", () => {
    expect(formatDuration(1000, 1030)).toBe("30s")
  })

  it("formats minute-scale durations", () => {
    expect(formatDuration(1000, 1090)).toBe("1m 30s")
  })

  it("formats hour-scale durations", () => {
    expect(formatDuration(1000, 1000 + 3661)).toBe("1h 1m")
  })

  it("clamps negative durations to zero", () => {
    expect(formatDuration(1030, 1000)).toBe("0s")
  })

  it("returns a dash when start or end cannot be parsed", () => {
    expect(formatDuration("bad", 1000)).toBe("—")
  })
})

describe("formatBytes", () => {
  it("handles zero", () => {
    expect(formatBytes(0)).toBe("0 Bytes")
  })

  it("formats byte-scale values", () => {
    expect(formatBytes(512)).toBe("512.0 Bytes")
  })

  it("formats kilobytes", () => {
    expect(formatBytes(1024)).toBe("1.0 KB")
  })

  it("formats megabytes", () => {
    expect(formatBytes(1024 * 1024 * 2.5)).toBe("2.5 MB")
  })

  it("formats gigabytes", () => {
    expect(formatBytes(1024 ** 3)).toBe("1.0 GB")
  })
})

describe("errorMessageFrom", () => {
  function response(body: string, init: { status?: number; statusText?: string } = {}): Response {
    return new Response(body, {
      status: init.status ?? 500,
      statusText: init.statusText ?? "Internal Server Error",
    })
  }

  it("returns the server JSON message when present", async () => {
    const res = response(JSON.stringify({ message: "config not found" }), { status: 404 })
    expect(await errorMessageFrom(res)).toBe("config not found")
  })

  it("falls back to the status line when the body is empty", async () => {
    const res = response("", { status: 503, statusText: "Service Unavailable" })
    expect(await errorMessageFrom(res)).toBe("503 Service Unavailable")
  })

  it("falls back to the status line when JSON has no message field", async () => {
    const res = response(JSON.stringify({ error: "nope" }), { status: 400, statusText: "Bad Request" })
    expect(await errorMessageFrom(res)).toBe("400 Bad Request")
  })

  it("returns the raw body when it is not JSON", async () => {
    const res = response("plain text failure", { status: 500 })
    expect(await errorMessageFrom(res)).toBe("plain text failure")
  })

  it("truncates very long messages", async () => {
    const long = "x".repeat(400)
    const res = response(JSON.stringify({ message: long }), { status: 500 })
    const result = await errorMessageFrom(res)
    expect(result).toHaveLength(300)
    expect(result.endsWith("…")).toBe(true)
  })
})
