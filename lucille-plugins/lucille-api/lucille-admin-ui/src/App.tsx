import { Routes, Route } from "react-router-dom"
import { ErrorFallback } from "./components/error-fallback/error-fallback"
import Layout from "./components/layout/layout"
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
      </Routes>
    </ErrorFallback>
  )
}

export default App
