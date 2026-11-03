import { useState, type KeyboardEvent } from "react"
import { Navigate, useLocation, useNavigate } from "react-router-dom"
import { Database } from "lucide-react"
import { Button } from "@/components/ui/button/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card/card"
import { useAuth } from "@/lib/auth-context"
import styles from "./login.module.css"

interface LocationState {
  from?: { pathname: string }
}

export default function Login() {
  const { status, login } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()

  const [password, setPassword] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const from = (location.state as LocationState | null)?.from?.pathname ?? "/"

  // If auth isn't required (or we're already signed in), there's nothing to log into.
  if (status === "authenticated") {
    return <Navigate to={from} replace />
  }

  const handleSubmit = async () => {
    if (submitting) return
    setError(null)
    setSubmitting(true)
    try {
      await login(password)
      navigate(from, { replace: true })
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed.")
    } finally {
      setSubmitting(false)
    }
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      handleSubmit()
    }
  }

  return (
    <div className={styles.page}>
      <Card className={styles.card}>
        <CardHeader className={styles.header}>
          <div className={styles.brand}>
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </div>
          <CardTitle>Sign in</CardTitle>
          <CardDescription>Enter the admin password to access the console.</CardDescription>
        </CardHeader>
        <CardContent>
          <div className={styles.form}>
            <label className={styles.field}>
              <span className={styles.label}>Password</span>
              <input
                className={styles.input}
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                onKeyDown={handleKeyDown}
                autoComplete="current-password"
                autoFocus
              />
            </label>

            {error && <div className={styles.error} role="alert">{error}</div>}

            <Button variant="default" onClick={handleSubmit} disabled={submitting}>
              {submitting ? "Signing in…" : "Sign in"}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
