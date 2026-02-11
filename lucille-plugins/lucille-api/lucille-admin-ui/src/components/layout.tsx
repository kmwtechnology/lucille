import { Outlet } from "react-router-dom"
import Sidebar from "./sidebar"

export default function Layout() {
  return (
    <div className="flex min-h-screen" style={{ background: "linear-gradient(135deg, #f0fafa, #f0f7ff)" }}>
      <Sidebar />
      <main className="flex-1 w-0">
        <Outlet />
      </main>
    </div>
  )
}
