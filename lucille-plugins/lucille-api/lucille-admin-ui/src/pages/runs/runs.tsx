import { Link } from "@/components/ui/link/link"
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card/card"
import { Play } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import { formatDuration, formatInstant } from "@/lib/utils"
import type { Run } from "@/types/api"
import { RunStatusBadge } from "@/components/run-status/run-status"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import { EmptyState } from "@/components/empty-state/empty-state"
import { ViewLink } from "@/components/view-link/view-link"
import styles from "./runs.module.css"

function startTimeMs(run: Run): number {
  return run.startTime ? new Date(run.startTime).getTime() : 0
}

function StartRunLink() {
  return (
    <Link className={styles.startButton} to="/runs/detail?id=new">
      <Play className="mr-2 h-4 w-4 flex-shrink-0" /> Start New Run
    </Link>
  )
}

function RunCard({ run }: { run: Run }) {
  return (
    <Card className={styles.runCard}>
      <CardHeader className={styles.runHeader}>
        <CardTitle className={styles.runTitle}>
          <Play className="h-5 w-5 text-primary-600 flex-shrink-0" />
          <span className={styles.runId} title={run.runId}>
            {run.runId}
          </span>
        </CardTitle>
        <RunStatusBadge run={run} />
      </CardHeader>
      <CardContent className={styles.runContent}>
        <div className={styles.detailRow}>
          <span className={styles.detailLabel}>Config</span>
          <Link
            variant="link"
            size="sm"
            className={styles.configLink}
            to={`/configs/detail?id=${encodeURIComponent(run.configId)}`}
          >
            {run.configId}
          </Link>
        </div>
        <div className={styles.detailRow}>
          <span className={styles.detailLabel}>Started</span>
          <span className={styles.detailValue}>{formatInstant(run.startTime)}</span>
        </div>
        <div className={styles.detailRow}>
          <span className={styles.detailLabel}>Duration</span>
          <span className={styles.detailValue}>
            {run.done ? formatDuration(run.startTime, run.endTime) : "In progress"}
          </span>
        </div>
      </CardContent>
      <CardFooter>
        <ViewLink to={`/runs/detail?id=${encodeURIComponent(run.runId)}`} label="View" />
      </CardFooter>
    </Card>
  )
}

export default function Runs() {
  // Poll so in-progress runs update live.
  const runs = useFetch<Run[]>("/v1/run", 3000)

  const sorted =
    runs.status === "success"
      ? [...runs.data].sort((a, b) => startTimeMs(b) - startTimeMs(a))
      : []

  return (
    <div className={styles.page}>
      <div className={styles.pageHeader}>
        <div>
          <h1 className={styles.pageTitle}>Runs</h1>
          <p className={styles.pageSubtitle}>Monitor and start Lucille pipeline runs</p>
        </div>
        <StartRunLink />
      </div>

      {runs.status === "loading" ? (
        <LoadingText />
      ) : runs.status === "error" ? (
        <ErrorState title="An error has occurred" subtext="Unable to load runs data" />
      ) : sorted.length === 0 ? (
        <EmptyState
          icon={Play}
          title="No runs yet"
          subtext="Start a run from an existing configuration to see it here."
          action={<StartRunLink />}
        />
      ) : (
        <div className={styles.runGrid}>
          {sorted.map((run) => (
            <RunCard key={run.runId} run={run} />
          ))}
        </div>
      )}
    </div>
  )
}
