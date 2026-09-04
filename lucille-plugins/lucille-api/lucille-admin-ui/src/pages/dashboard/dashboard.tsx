import { Link } from "@/components/ui/link/link"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import {
  ArrowRight,
  Play,
  Settings,
  Activity,
  CheckCircle,
  Database,
  Server,
  XCircle,
} from "lucide-react"
import { formatBytes } from "@/lib/utils"
import { useFetch } from "@/hooks/use-fetch"
import { useHealthStatus } from "@/hooks/use-health-status"
import type { Run, SystemStats } from "@/types/api"
import styles from "./dashboard.module.css"

export default function Dashboard() {
  const status = useHealthStatus()
  const configs = useFetch<Record<string, unknown>>("/v1/config")
  const runs = useFetch<Run[]>("/v1/run")
  const stats = useFetch<SystemStats>("/v1/systemstats", 5000)

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
          Manage your data pipelines, configurations, and system health
        </p>
      </div>

      {/* Row 1: Status cards */}
      <div className={styles.statusGrid}>
        {/* API Status */}
        <Card className={`${styles.statCard} ${status === "unavailable" ? styles.statCardUnhealthy : styles.statCardHealthy}`}>
          <CardHeader className={styles.statHeader}>
            <CardTitle className={status === "unavailable" ? styles.statTitleUnhealthy : styles.statTitleHealthy}>
              {status === "loading" ? (
                <Activity className="h-5 w-5 animate-spin text-teal-500" />
              ) : status === "healthy" ? (
                <CheckCircle className="h-5 w-5 text-teal-500" />
              ) : (
                <XCircle className="h-5 w-5 text-red-500" />
              )}
              <span>API Status</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="flex-1">
            {status === "loading" ? (
              <div className={styles.statValueTeal}>Checking...</div>
            ) : status === "healthy" ? (
              <>
                <div className={styles.statValueTeal}>Healthy</div>
                <div className={styles.statSubtextTeal}>All systems operational</div>
              </>
            ) : status === "unavailable" ? (
              <>
                <div className={styles.statValueRed}>Unhealthy</div>
                <div className={styles.statSubtextRed}>Lucille API server is unavailable.</div>
              </>
            ) : (
              <>
                <div className={styles.statValueYellow}>Not Ready</div>
                <div className={styles.statSubtextYellow}>
                  Lucille API server is not ready to take requests yet.
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Storage */}
        <Card className={styles.statCardBlue}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5 text-blue-500" />
              Storage
            </CardTitle>
          </CardHeader>
          <CardContent>
            {stats.status === "loading" ? (
              <div className={styles.statValueBlue}>Loading...</div>
            ) : stats.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load storage data</div>
              </>
            ) : (
              <>
                <div className={styles.statValueBlue}>{stats.data.storage.percent.toFixed(0)}%</div>
                <div className={styles.statSubtext}>{formatBytes(stats.data.storage.available)} free of {formatBytes(stats.data.storage.total)}</div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Memory */}
        <Card className={styles.statCardTeal}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5 text-teal-500" />
              Memory
            </CardTitle>
          </CardHeader>
          <CardContent>
            {stats.status === "loading" ? (
              <div className={styles.statValueTeal}>Loading...</div>
            ) : stats.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load memory data</div>
              </>
            ) : (
              <>
                <div className={styles.statValueTeal}>{stats.data.ram.percent.toFixed(0)}%</div>
                <div className={styles.statSubtext}>{formatBytes(stats.data.ram.used)} used of {formatBytes(stats.data.ram.total)}</div>
              </>
            )}
          </CardContent>
        </Card>

        {/* CPU */}
        <Card className={styles.statCardBlue}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-blue-500" />
              CPU
            </CardTitle>
          </CardHeader>
          <CardContent>
            {stats.status === "loading" ? (
              <div className={styles.statValueBlue}>Loading...</div>
            ) : stats.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load CPU data</div>
              </>
            ) : (
              <>
                <div className={styles.statValueBlue}>{stats.data.cpu.percent.toFixed(0)}%</div>
                <div className={styles.statSubtext}>Load Avg: {stats.data.cpu.loadAverage.toFixed(2)}</div>
              </>
            )}
          </CardContent>
        </Card>

        {/* JVM */}
        <Card className={styles.statCardOrange}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5 text-orange-500" />
              JVM
            </CardTitle>
          </CardHeader>
          <CardContent>
            {stats.status === "loading" ? (
              <div className={styles.statValueOrange}>Loading...</div>
            ) : stats.status === "error" ? (
              <>
                <div className={styles.statValueRed}>An error has occurred</div>
                <div className={styles.statSubtextRed}>Unable to load JVM data</div>
              </>
            ) : (
              <>
                <div className={styles.statValueOrange}>{stats.data.jvm.percent.toFixed(0)}%</div>
                <div className={styles.statSubtext}>{formatBytes(stats.data.jvm.used)} used of {formatBytes(stats.data.jvm.total)}</div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Row 2: Detail cards */}
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
            <Link variant="ghost" size="sm" className={styles.cardFooterButton} to="/configs">
              Manage Configs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
            </Link>
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
            <Link variant="ghost" size="sm" className={styles.cardFooterButton} to="/runs">
              View Runs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
            </Link>
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
            <Link className={styles.actionButtonPrimary} to="/configs/detail?id=new">
              <Settings className="mr-2 h-4 w-4 flex-shrink-0" /> Create New Configuration
            </Link>
            <Link className={styles.actionButtonSecondary} to="/runs?id=new">
              <Play className="mr-2 h-4 w-4 flex-shrink-0" /> Start New Run
            </Link>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
