import { Navigate, Outlet, useLocation } from "react-router-dom"
import { useAuth } from "@/lib/auth-context"
import { LoadingText } from "@/components/loading-text/loading-text"

/**
 * Route guard. While the auth check is in flight it renders nothing of consequence; once resolved
 * it renders the app when authenticated (including when the API requires no auth) or redirects to
 * /login otherwise, remembering the attempted location.
 */
export default function RequireAuth() {
  const { status } = useAuth()
  const location = useLocation()

  if (status === "checking") {
    return <LoadingText />
  }

  if (status === "unauthenticated") {
    return <Navigate to="/login" replace state={{ from: location }} />
  }

  return <Outlet />
}
