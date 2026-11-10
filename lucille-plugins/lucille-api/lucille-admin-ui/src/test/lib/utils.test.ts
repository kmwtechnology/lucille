// Tests the lib/utils helpers: class merging, timestamp parsing/formatting, and API error extraction.
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
  // Multiple class strings are concatenated with a space.
  it("joins class names", () => {
    expect(cn("a", "b")).toBe("a b")
  })

  // Conditional inputs (false/undefined/null) are skipped so no empty tokens leak in.
  it("drops falsy values", () => {
    expect(cn("a", false, undefined, null, "b")).toBe("a b")
  })

  // tailwind-merge resolves same-property conflicts, keeping the last class.
  it("merges conflicting tailwind classes, last one wins", () => {
    expect(cn("px-2", "px-4")).toBe("px-4")
  })
})

describe("toEpochMs", () => {
  // Absent values are NaN so callers can detect "no timestamp".
  it("returns NaN for null, undefined, and empty string", () => {
    expect(toEpochMs(null)).toBeNaN()
    expect(toEpochMs(undefined)).toBeNaN()
    expect(toEpochMs("")).toBeNaN()
  })

  // The API sends epoch seconds, so a number is scaled up to milliseconds.
  it("treats a number as epoch seconds and converts to ms", () => {
    expect(toEpochMs(1_700_000_000)).toBe(1_700_000_000_000)
  })

  // A numeric string is treated the same as a number (epoch seconds -> ms).
  it("treats a numeric string as epoch seconds", () => {
    expect(toEpochMs("1700000000")).toBe(1_700_000_000_000)
  })

  // A non-numeric string is parsed as an ISO date instead.
  it("parses an ISO date string as a date", () => {
    expect(toEpochMs("2024-01-01T00:00:00.000Z")).toBe(Date.parse("2024-01-01T00:00:00.000Z"))
  })

  // Garbage that is neither a number nor a date is NaN.
  it("returns NaN for an unparseable string", () => {
    expect(toEpochMs("not-a-date")).toBeNaN()
  })
})

describe("formatInstant", () => {
  // No timestamp renders as an em dash placeholder.
  it("returns a dash when the value is absent", () => {
    expect(formatInstant(null)).toBe("—")
    expect(formatInstant(undefined)).toBe("—")
    expect(formatInstant("")).toBe("—")
  })

  // A valid instant is rendered via the locale date-time formatter.
  it("formats a valid epoch-seconds number to a locale string", () => {
    const expected = new Date(1_700_000_000_000).toLocaleString()
    expect(formatInstant(1_700_000_000)).toBe(expected)
  })

  // An unparseable value is shown verbatim rather than as "Invalid Date".
  it("falls back to the raw string when unparseable", () => {
    expect(formatInstant("garbage")).toBe("garbage")
  })
})

describe("formatDuration", () => {
  // Without a start time there is no duration to show.
  it("returns a dash when start is absent", () => {
    expect(formatDuration(null, null)).toBe("—")
    expect(formatDuration("", 123)).toBe("—")
  })

  // Under a minute is shown as plain seconds.
  it("formats sub-minute durations in seconds", () => {
    expect(formatDuration(1000, 1030)).toBe("30s")
  })

  // A minute or more adds a minutes component.
  it("formats minute-scale durations", () => {
    expect(formatDuration(1000, 1090)).toBe("1m 30s")
  })

  // An hour or more adds an hours component (seconds are dropped at this scale).
  it("formats hour-scale durations", () => {
    expect(formatDuration(1000, 1000 + 3661)).toBe("1h 1m")
  })

  // An end before the start clamps to 0 rather than showing a negative duration.
  it("clamps negative durations to zero", () => {
    expect(formatDuration(1030, 1000)).toBe("0s")
  })

  // An unparseable endpoint yields a dash instead of a bogus number.
  it("returns a dash when start or end cannot be parsed", () => {
    expect(formatDuration("bad", 1000)).toBe("—")
  })
})

describe("formatBytes", () => {
  // Zero is a special case with its own label.
  it("handles zero", () => {
    expect(formatBytes(0)).toBe("0 Bytes")
  })

  // Values under 1 KB stay in bytes.
  it("formats byte-scale values", () => {
    expect(formatBytes(512)).toBe("512.0 Bytes")
  })

  // Scales to KB at 1024 bytes.
  it("formats kilobytes", () => {
    expect(formatBytes(1024)).toBe("1.0 KB")
  })

  // Scales to MB and keeps one decimal place.
  it("formats megabytes", () => {
    expect(formatBytes(1024 * 1024 * 2.5)).toBe("2.5 MB")
  })

  // Scales to GB at 1024^3.
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

  // A JSON error body's `message` field is the preferred message.
  it("returns the server JSON message when present", async () => {
    const res = response(JSON.stringify({ message: "config not found" }), { status: 404 })
    expect(await errorMessageFrom(res)).toBe("config not found")
  })

  // With no body there is nothing to show but the HTTP status line.
  it("falls back to the status line when the body is empty", async () => {
    const res = response("", { status: 503, statusText: "Service Unavailable" })
    expect(await errorMessageFrom(res)).toBe("503 Service Unavailable")
  })

  // JSON without a `message` field also falls back to the status line.
  it("falls back to the status line when JSON has no message field", async () => {
    const res = response(JSON.stringify({ error: "nope" }), { status: 400, statusText: "Bad Request" })
    expect(await errorMessageFrom(res)).toBe("400 Bad Request")
  })

  // A plain-text (non-JSON) body is surfaced as-is. Covers the API's string 400s.
  it("returns the raw body when it is not JSON", async () => {
    const res = response("plain text failure", { status: 500 })
    expect(await errorMessageFrom(res)).toBe("plain text failure")
  })

  // Over-long messages are capped and ellipsized so the UI can't be flooded.
  it("truncates very long messages", async () => {
    const long = "x".repeat(400)
    const res = response(JSON.stringify({ message: long }), { status: 500 })
    const result = await errorMessageFrom(res)
    expect(result).toHaveLength(300)
    expect(result.endsWith("…")).toBe(true)
  })
})
