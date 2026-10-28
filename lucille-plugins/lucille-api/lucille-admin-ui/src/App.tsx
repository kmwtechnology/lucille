import { Routes, Route } from "react-router-dom"
import { ErrorFallback } from "./components/error-fallback/error-fallback"
import RequireAuth from "./components/require-auth/require-auth"
import Layout from "./components/layout/layout"
import Login from "./pages/login/login"
import Dashboard from "./pages/dashboard/dashboard"
import Configs from "./pages/configs/configs"
import ConfigCreate from "./pages/config-create/config-create"
import ConfigDetail from "./pages/config-detail/config-detail"
import Runs from "./pages/runs/runs"
import RunDetail, { StartRun } from "./pages/run-detail/run-detail"
import NotFound from "./pages/not-found/not-found"

function App() {
  return (
    <ErrorFallback>
      <Routes>
        {/* Public: the login page is reachable without a token. */}
        <Route path="/login" element={<Login />} />

        {/* Everything else is behind the auth guard. */}
        <Route element={<RequireAuth />}>
          <Route element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="configs" element={<Configs />} />
            <Route path="configs/create" element={<ConfigCreate />} />
            <Route path="configs/:id" element={<ConfigDetail />} />
            <Route path="runs" element={<Runs />} />
            <Route path="runs/new" element={<StartRun />} />
            <Route path="runs/:id" element={<RunDetail />} />
            <Route path="*" element={<NotFound />} />
          </Route>
        </Route>
      </Routes>
    </ErrorFallback>
  )
}

export default App
