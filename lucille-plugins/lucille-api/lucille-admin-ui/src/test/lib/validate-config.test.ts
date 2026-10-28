import { describe, it, expect } from "vitest"
import { validateConfigText } from "@/lib/validate-config"

describe("validateConfigText", () => {
  it("rejects empty or whitespace-only text", () => {
    expect(validateConfigText("")).toEqual({
      valid: false,
      message: "Configuration cannot be empty.",
    })
    expect(validateConfigText("   \n\t ")).toMatchObject({ valid: false })
  })

  it("accepts balanced braces and brackets", () => {
    expect(validateConfigText("{ a: [1, 2, 3], b: { c: 1 } }")).toEqual({ valid: true })
  })

  it("flags an unexpected closing bracket", () => {
    const result = validateConfigText("}")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unexpected closing '}'")
  })

  it("flags mismatched brackets", () => {
    const result = validateConfigText("{ a: [1, 2 }")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Mismatched bracket")
  })

  it("flags unbalanced brackets with a missing closer", () => {
    const result = validateConfigText("{ a: 1")
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("missing '}'")
  })

  it("ignores brackets inside double-quoted strings", () => {
    expect(validateConfigText('{ a: "a } b ] c" }')).toEqual({ valid: true })
  })

  it("respects escaped quotes inside strings", () => {
    expect(validateConfigText('{ a: "he said \\"} ]\\"" }')).toEqual({ valid: true })
  })

  it("ignores brackets inside line comments", () => {
    expect(validateConfigText("{\n  # a comment with } and ]\n  a: 1\n}")).toEqual({ valid: true })
    expect(validateConfigText("{\n  // another } comment ]\n  a: 1\n}")).toEqual({ valid: true })
  })

  it("ignores brackets inside triple-quoted strings", () => {
    expect(validateConfigText('{ a: """raw } ] text""" }')).toEqual({ valid: true })
  })

  it("flags an unterminated triple-quoted string", () => {
    const result = validateConfigText('{ a: """never closed }')
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unterminated string")
  })

  it("flags an unterminated double-quoted string", () => {
    const result = validateConfigText('{ a: "never closed }')
    expect(result.valid).toBe(false)
    if (!result.valid) expect(result.message).toContain("Unterminated string")
  })
})
