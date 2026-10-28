import { Navigate, Outlet, useLocation } from "react-router-dom"
import { useAuth } from "@/lib/auth-context"

/**
 * Route guard. Renders the nested routes only when authenticated; otherwise redirects to /login,
 * remembering the attempted location so the user returns there after signing in.
 */
export default function RequireAuth() {
  const { isAuthenticated } = useAuth()
  const location = useLocation()

  if (!isAuthenticated) {
    return <Navigate to="/login" replace state={{ from: location }} />
  }

  return <Outlet />
}
