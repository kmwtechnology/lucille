import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react"
import { api } from "@/lib/api"
import {
  clearToken,
  getToken,
  setToken as persistToken,
  setUnauthorizedHandler,
} from "@/lib/auth-token"

interface AuthContextValue {
  isAuthenticated: boolean
  /** Logs in via the API and stores the token; throws on a bad password. */
  login: (password: string) => Promise<void>
  /** Clears the token, returning the user to the login screen. */
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

/**
 * Provides authentication state and keeps the sessionStorage-backed token in sync. Registers a
 * global 401 handler so an expired or invalid token logs the user out.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => getToken())

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

  const value = useMemo<AuthContextValue>(
    () => ({ isAuthenticated: token !== null, login, logout }),
    [token, login, logout],
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
