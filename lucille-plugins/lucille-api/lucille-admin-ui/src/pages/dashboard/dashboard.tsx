import { Link } from "@/components/ui/link/link"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import { Play, Settings } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import type { Run } from "@/types/api"
import { ViewLink } from "@/components/view-link/view-link"
import styles from "./dashboard.module.css"

export default function Dashboard() {
  const configs = useFetch<Record<string, unknown>>("/v1/config")
  const runs = useFetch<Run[]>("/v1/run")

  const totalConfigs = configs.status === "success" ? Object.keys(configs.data).length : null
  const runsList = runs.status === "success" ? runs.data : null
  const totalRunsCount = runsList?.length ?? 0
  const completedCount = runsList?.filter((r) => r.done).length ?? 0
  const runningCount = runsList?.filter((r) => !r.done).length ?? 0

  return (
    <div className={styles.page}>
      <div className={styles.pageHeader}>
        <h1 className={styles.pageTitle}>Lucille Admin Dashboard</h1>
        <p className={styles.pageSubtitle}>
          Manage your data pipelines and configurations
        </p>
      </div>

      {/* Detail cards */}
      <div className={styles.detailGrid}>
        {/* Configurations */}
        <Card className={styles.detailCard}>
          <CardHeader className={styles.detailHeader}>
            <CardTitle className={styles.detailTitle}>
              <Settings className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Configurations</span>
            </CardTitle>
            <CardDescription className={styles.detailDescription}>
              Manage your pipeline configurations
            </CardDescription>
          </CardHeader>
          <CardContent className={styles.detailContent}>
            {configs.status === "loading" ? (
              <div className={styles.loadingText}>Loading...</div>
            ) : configs.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load configuration data</div>
              </>
            ) : (
              <div className={styles.detailList}>
                <div className={styles.detailRow}>
                  <span className={styles.detailLabel}>Total Configs</span>
                  <span className={styles.detailValue}>{totalConfigs}</span>
                </div>
              </div>
            )}
          </CardContent>
          <CardFooter>
            <ViewLink to="/configs" label="Manage Configs" />
          </CardFooter>
        </Card>

        {/* Pipeline Runs */}
        <Card className={styles.detailCard}>
          <CardHeader className={styles.detailHeader}>
            <CardTitle className={styles.detailTitle}>
              <Play className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Pipeline Runs</span>
            </CardTitle>
            <CardDescription className={styles.detailDescription}>
              Monitor pipeline executions
            </CardDescription>
          </CardHeader>
          <CardContent className={styles.detailContent}>
            {runs.status === "loading" ? (
              <div className={styles.loadingText}>Loading...</div>
            ) : runs.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load runs data</div>
              </>
            ) : (
              <div className={styles.detailList}>
                <div className={styles.detailRow}>
                  <span className={styles.detailLabel}>Total Runs</span>
                  <span className={styles.detailValue}>{totalRunsCount}</span>
                </div>
                <div className={styles.detailRow}>
                  <span className={styles.detailLabel}>Completed</span>
                  <span className={styles.detailValue}>{completedCount}</span>
                </div>
                <div className={styles.detailRow}>
                  <span className={styles.detailLabel}>Running</span>
                  <span className={styles.badge}>{runningCount} Active</span>
                </div>
              </div>
            )}
          </CardContent>
          <CardFooter>
            <ViewLink to="/runs" label="View Runs" />
          </CardFooter>
        </Card>

        {/* Quick Actions */}
        <Card className={styles.detailCardStatic}>
          <CardHeader>
            <CardTitle className={styles.quickActionsTitle}>Quick Actions</CardTitle>
            <CardDescription className={styles.detailDescription}>
              Common operations
            </CardDescription>
          </CardHeader>
          <CardContent className={styles.quickActionsContent}>
            <Link className={styles.actionButtonPrimary} to="/configs/create">
              <Settings className="mr-2 h-4 w-4 flex-shrink-0" />
              <span className={styles.actionButtonLabel}>Create New Configuration</span>
            </Link>
            <Link className={styles.actionButtonSecondary} to="/runs/new">
              <Play className="mr-2 h-4 w-4 flex-shrink-0" />
              <span className={styles.actionButtonLabel}>Start New Run</span>
            </Link>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
