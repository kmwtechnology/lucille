import { Outlet } from "react-router-dom"
import Sidebar from "../sidebar/sidebar"
import { ApiUnavailable } from "../api-unavailable/api-unavailable"
import { useHealthStatus } from "@/hooks/use-health-status"
import styles from "./layout.module.css"

export default function Layout() {
  const status = useHealthStatus()

  return (
    <div className={styles.wrapper}>
      <Sidebar />
      <main className={styles.main}>
        {status === "unavailable" ? <ApiUnavailable /> : <Outlet />}
      </main>
    </div>
  )
}
