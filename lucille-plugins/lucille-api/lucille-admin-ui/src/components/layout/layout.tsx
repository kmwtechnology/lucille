import { Outlet } from "react-router-dom"
import Sidebar from "../sidebar/sidebar"
import styles from "./layout.module.css"

export default function Layout() {
  return (
    <div className={styles.wrapper}>
      <Sidebar />
      <main className={styles.main}>
        <Outlet />
      </main>
    </div>
  )
}
