import { useNavigate } from "react-router-dom"
import { Link } from "@/components/ui/link/link"
import { Link as LinkIcon, Play } from "lucide-react"
import { useFetch } from "@/hooks/use-fetch"
import { formatDuration, formatInstant } from "@/lib/utils"
import type { Run } from "@/types/api"
import { RunStatusBadge } from "@/components/run-status/run-status"
import { LoadingText } from "@/components/loading-text/loading-text"
import { ErrorState } from "@/components/error-state/error-state"
import { EmptyState } from "@/components/empty-state/empty-state"
import { Pagination } from "@/components/pagination/pagination"
import { usePagination } from "@/hooks/use-pagination"
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

function RunRow({ run }: { run: Run }) {
  const navigate = useNavigate()
  const to = `/runs/detail?id=${encodeURIComponent(run.runId)}`

  return (
    <tr
      className={styles.row}
      onClick={() => navigate(to)}
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter") navigate(to)
      }}
    >
      <td className={styles.idCell}>
        <span className={styles.idWrap}>
          <LinkIcon className={styles.idIcon} aria-hidden="true" />
          <span className={styles.rowId} title={run.runId}>
            {run.runId}
          </span>
        </span>
      </td>
      <td>
        <RunStatusBadge run={run} />
      </td>
      <td className={styles.idCell}>
        <span className={styles.configId} title={run.configId}>
          {run.configId}
        </span>
      </td>
      <td className={styles.nowrap}>{formatInstant(run.startTime)}</td>
      <td className={styles.nowrap}>
        {run.done ? formatDuration(run.startTime, run.endTime) : "In progress"}
      </td>
    </tr>
  )
}

export default function Runs() {
  const runs = useFetch<Run[]>("/v1/run", 3000)

  const sorted =
    runs.status === "success"
      ? [...runs.data].sort((a, b) => startTimeMs(b) - startTimeMs(a))
      : []
  const pagination = usePagination(sorted, 10)

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
        <>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th className={styles.th}>Run ID</th>
                  <th className={styles.th}>Status</th>
                  <th className={styles.th}>Config</th>
                  <th className={styles.th}>Started</th>
                  <th className={styles.th}>Duration</th>
                </tr>
              </thead>
              <tbody>
                {pagination.pageItems.map((run) => (
                  <RunRow key={run.runId} run={run} />
                ))}
              </tbody>
            </table>
          </div>
          <Pagination state={pagination} label="runs" />
        </>
      )}
    </div>
  )
}
