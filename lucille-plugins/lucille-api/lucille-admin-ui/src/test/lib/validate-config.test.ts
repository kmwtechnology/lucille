// Tests client-side config validation: bracket balance, strings, and comments.
import { describe, it, expect } from "vitest"
import { validateConfigText } from "@/lib/validate-config"

describe("validateConfigText", () => {
  // An empty or all-whitespace config is rejected before any parsing.
  it("rejects empty or whitespace-only text", () => {
    expect(validateConfigText("")).toEqual({
      valid: false,
      message: "Configuration cannot be empty.",
    })
    expect(validateConfigText("   \n\t ")).toMatchObject({ valid: false })
  })

  // Properly matched and nested braces/brackets pass.
  it("accepts balanced braces and brackets", () => {
    expect(validateConfigText("{ a: [1, 2, 3], b: { c: 1 } }")).toEqual({ valid: true })
  })

  // A closer with no matching opener is reported.
  it("flags an unexpected closing bracket", () => {
    const result = validateConfigText("}")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unexpected closing '}'")
  })

  // A closer of the wrong type (]} vs }]) is reported as a mismatch.
  it("flags mismatched brackets", () => {
    const result = validateConfigText("{ a: [1, 2 }")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Mismatched bracket")
  })

  // An opener that is never closed is reported.
  it("flags unbalanced brackets with a missing closer", () => {
    const result = validateConfigText("{ a: 1")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("missing '}'")
  })

  // Brackets inside a double-quoted string are literal text, not structure.
  it("ignores brackets inside double-quoted strings", () => {
    expect(validateConfigText('{ a: "a } b ] c" }')).toEqual({ valid: true })
  })

  // An escaped quote does not end the string, so its brackets stay ignored.
  it("respects escaped quotes inside strings", () => {
    expect(validateConfigText('{ a: "he said \\"} ]\\"" }')).toEqual({ valid: true })
  })

  // Brackets after # or // to end of line are comments, not structure.
  it("ignores brackets inside line comments", () => {
    expect(validateConfigText("{\n  # a comment with } and ]\n  a: 1\n}")).toEqual({ valid: true })
    expect(validateConfigText("{\n  // another } comment ]\n  a: 1\n}")).toEqual({ valid: true })
  })

  // HOCON triple-quoted (raw) strings have their brackets ignored too.
  it("ignores brackets inside triple-quoted strings", () => {
    expect(validateConfigText('{ a: """raw } ] text""" }')).toEqual({ valid: true })
  })

  // A triple-quoted string with no closing """ is reported.
  it("flags an unterminated triple-quoted string", () => {
    const result = validateConfigText('{ a: """never closed }')
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unterminated string")
  })

  // A double-quoted string with no closing " is reported.
  it("flags an unterminated double-quoted string", () => {
    const result = validateConfigText('{ a: "never closed }')
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unterminated string")
  })
})
