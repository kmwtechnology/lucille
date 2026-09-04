import { Routes, Route } from "react-router-dom"
import { ErrorFallback } from "./components/error-fallback/error-fallback"
import Layout from "./components/layout/layout"
import Dashboard from "./pages/dashboard/dashboard"

function Placeholder({ title }: { title: string }) {
  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold">{title}</h1>
      <p className="mt-2 text-muted-foreground">Coming soon</p>
    </div>
  )
}

function App() {
  return (
    <ErrorFallback>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="configs" element={<Placeholder title="Configurations" />} />
          <Route path="configs/detail" element={<Placeholder title="Configuration Detail" />} />
          <Route path="configs/connectors" element={<Placeholder title="Connectors" />} />
          <Route path="configs/stages" element={<Placeholder title="Pipeline Stages" />} />
          <Route path="configs/indexers" element={<Placeholder title="Indexers" />} />
          <Route path="runs" element={<Placeholder title="Runs" />} />
        </Route>
      </Routes>
    </ErrorFallback>
  )
}

export default App
