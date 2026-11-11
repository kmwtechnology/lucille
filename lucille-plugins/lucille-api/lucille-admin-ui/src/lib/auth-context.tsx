import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react"
import { api } from "@/lib/api"
import {
  clearToken,
  getToken,
  setToken as persistToken,
  setUnauthorizedHandler,
} from "@/lib/auth-token"

type AuthStatus = "checking" | "authenticated" | "unauthenticated"

interface AuthContextValue {
  status: AuthStatus
  /** Whether the API requires authentication (null while still checking). */
  authRequired: boolean | null
  /** Logs in via the API and stores the token; throws on a bad password. */
  login: (password: string) => Promise<void>
  /** Clears the token, returning the user to the login screen. */
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

/**
 * Provides authentication state. On load it asks the API whether auth is required: when it is not,
 * the app is usable with no login; when it is, a stored token authenticates and a 401 logs out.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => getToken())
  const [authRequired, setAuthRequired] = useState<boolean | null>(null)

  const logout = useCallback(() => {
    clearToken()
    setTokenState(null)
  }, [])

  const login = useCallback(async (password: string) => {
    const newToken = await api.login(password)
    persistToken(newToken)
    setTokenState(newToken)
  }, [])

  useEffect(() => {
    setUnauthorizedHandler(logout)
    return () => setUnauthorizedHandler(null)
  }, [logout])

  useEffect(() => {
    let cancelled = false
    api
      .getAuthInfo()
      .then((required) => {
        if (!cancelled) setAuthRequired(required)
      })
      .catch(() => {
        // If the check fails, assume auth is required so we fail closed (show login) rather than open.
        if (!cancelled) setAuthRequired(true)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const status: AuthStatus =
    authRequired === null ? "checking"
      : !authRequired ? "authenticated"
        : token !== null ? "authenticated"
          : "unauthenticated"

  const value = useMemo<AuthContextValue>(
    () => ({ status, authRequired, login, logout }),
    [status, authRequired, login, logout],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

/** Access the auth context. Throws if used outside an AuthProvider. */
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (ctx === null) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return ctx
}
