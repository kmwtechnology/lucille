export type ConfigValidation = { valid: true } | { valid: false; message: string }

const OPEN_TO_CLOSE: Record<string, string> = { "{": "}", "[": "]" }
const CLOSE_TO_OPEN: Record<string, string> = { "}": "{", "]": "[" }

/**
 * Lightweight, parser-free validation for the config editor. Only checks two
 * things, so it never rejects otherwise-valid HOCON:
 *  1. the text is not empty, and
 *  2. braces and brackets are balanced and correctly nested.
 *
 * Brackets inside double-quoted strings and comments (# or //) are ignored so
 * they don't skew the balance. Anything deeper is left to the server.
 */
export function validateConfigText(text: string): ConfigValidation {
  if (text.trim() === "") {
    return { valid: false, message: "Configuration cannot be empty." }
  }

  const stack: string[] = []
  let inString = false
  let inLineComment = false

  for (let i = 0; i < text.length; i++) {
    const ch = text[i]
    const next = text[i + 1]

    if (inLineComment) {
      if (ch === "\n") inLineComment = false
      continue
    }

    if (inString) {
      if (ch === "\\") {
        i++
      } else if (ch === '"') {
        inString = false
      }
      continue
    }

    if (ch === '"') {
      inString = true
    } else if (ch === "#" || (ch === "/" && next === "/")) {
      inLineComment = true
    } else if (ch === "{" || ch === "[") {
      stack.push(ch)
    } else if (ch === "}" || ch === "]") {
      const expectedOpen = CLOSE_TO_OPEN[ch]
      const lastOpen = stack.pop()
      if (lastOpen === undefined) {
        return { valid: false, message: `Unexpected closing '${ch}'.` }
      }
      if (lastOpen !== expectedOpen) {
        return {
          valid: false,
          message: `Mismatched bracket: expected '${OPEN_TO_CLOSE[lastOpen]}' but found '${ch}'.`,
        }
      }
    }
  }

  if (inString) {
    return { valid: false, message: "Unterminated string (missing closing quote)." }
  }

  if (stack.length > 0) {
    const open = stack[stack.length - 1]
    return { valid: false, message: `Unbalanced brackets: missing '${OPEN_TO_CLOSE[open]}'.` }
  }

  return { valid: true }
}
