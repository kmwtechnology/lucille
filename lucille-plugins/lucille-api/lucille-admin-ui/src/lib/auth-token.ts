/**
 * Framework-agnostic holder for the bearer token, shared by the React auth context and the fetch
 * layer (api.ts, use-fetch.ts) so neither has to prop-drill the token.
 *
 * The token is kept in sessionStorage: it survives a page refresh but is cleared when the tab
 * closes, and the API's signing key is regenerated on restart so tokens are short-lived anyway.
 *
 * Security note: a token in sessionStorage is readable by any JavaScript on the page, so an XSS
 * flaw could exfiltrate it. This is the accepted tradeoff for the chosen bearer-token approach; an
 * HttpOnly cookie would remove that exposure but requires server-side session handling.
 */

const STORAGE_KEY = "lucille.admin.token"

let unauthorizedHandler: (() => void) | null = null

/** Returns the current bearer token, or null when not logged in. */
export function getToken(): string | null {
  try {
    return sessionStorage.getItem(STORAGE_KEY)
  } catch {
    return null
  }
}

/** Stores the bearer token. */
export function setToken(token: string): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, token)
  } catch {
    // sessionStorage unavailable (private mode); token won't persist across a refresh
  }
}

/** Clears the stored bearer token. */
export function clearToken(): void {
  try {
    sessionStorage.removeItem(STORAGE_KEY)
  } catch {
    // ignore
  }
}

/**
 * Registers the callback invoked when a request comes back 401 (expired or invalid token). The
 * auth context uses this to clear the token and route the user back to the login page.
 */
export function setUnauthorizedHandler(handler: (() => void) | null): void {
  unauthorizedHandler = handler
}

/** Invoked by the fetch layer on a 401 response. */
export function notifyUnauthorized(): void {
  unauthorizedHandler?.()
}

/** Builds the Authorization header when a token is present, else an empty object. */
export function authHeader(): Record<string, string> {
  const token = getToken()
  return token ? { Authorization: `Bearer ${token}` } : {}
}
