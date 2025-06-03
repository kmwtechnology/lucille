"use client"

import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { useEffect, useState } from "react"
import type { Run } from "../../types/run";
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Plus, ArrowRight, Clock, Calendar, X, Check, Play } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

interface ConfigItem { id: string }

import { Suspense } from "react";

export default function RunsUnifiedPage() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <RunsUnifiedPageContent />
    </Suspense>
  );
}

function RunsUnifiedPageContent() {
  const searchParams = useSearchParams()

  const runId = searchParams.get('id')
  const configIdFilter = searchParams.get('configId')
  const isNewRun = runId === 'new'

  // State for runs list
  const [runs, setRuns] = useState<Run[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const filteredRuns = configIdFilter ? runs.filter(run => run.configId === configIdFilter) : runs;

  // State for configs (for new run)
  const [configs, setConfigs] = useState<ConfigItem[]>([])
  const [configsLoading, setConfigsLoading] = useState(false)
  const [configsError, setConfigsError] = useState<string | null>(null)
  const [statuses, setStatuses] = useState<Record<string, 'idle' | 'loading' | 'started'>>({});

  // State for single run detail
  const [runDetail, setRunDetail] = useState<Run | null>(null)
  const [runDetailLoading, setRunDetailLoading] = useState(false)
  const [runDetailError, setRunDetailError] = useState("")

  // Fetch runs list
  useEffect(() => {
    if (!runId) {
      setLoading(true)
      setError("")
      fetch('http://localhost:8080/v1/run')
  .then(res => {
    if (!res.ok) throw new Error('Lucille Unavailable')
    return res.json()
  })
  .then(data => {
    console.log('Fetched runs:', data);
    setRuns(Array.isArray(data) ? data : []);
  })
  .catch(err => {
    if (
      (err instanceof TypeError && err.message === 'Failed to fetch') ||
      (err instanceof Error && err.message === 'Lucille Unavailable')
    ) {
      setError('Lucille API unavailable. Please check your server connection.');
    } else {
      setError(err instanceof Error ? err.message : JSON.stringify(err));
      // Only log truly unexpected errors
      console.error('Unexpected error fetching runs:', err);
    }
  })
  .finally(() => setLoading(false))
    }
  }, [runId])

  // Fetch configs for new run
  useEffect(() => {
    if (isNewRun) {
      setConfigsLoading(true)
      setConfigsError(null)
      fetch('http://localhost:8080/v1/config', { cache: 'no-store' })
        .then(res => {
          if (!res.ok) throw new Error('Failed to load configs')
          return res.json()
        })
        .then(data => setConfigs(Object.keys(data || {}).map(id => ({ id }))))
        .catch(() => setConfigsError('Error loading configs'))
        .finally(() => setConfigsLoading(false))
    }
  }, [isNewRun])

  // Fetch single run detail
  useEffect(() => {
    if (runId && !isNewRun) {
      setRunDetailLoading(true)
      setRunDetailError("")
      fetch(`http://localhost:8080/v1/run/${runId}`)
        .then(res => {
          if (!res.ok) throw new Error('Run not found')
          return res.json()
        })
        .then(data => setRunDetail(data))
        .catch(err => setRunDetailError(err instanceof Error ? err.message : 'Unknown error'))
        .finally(() => setRunDetailLoading(false))
    }
  }, [runId, isNewRun])

  // Handlers
  function handleRun(configId: string) {
    setStatuses(prev => ({ ...prev, [configId]: 'loading' }))
    fetch('http://localhost:8080/v1/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ configId })
    })
      .then(res => {
        if (!res.ok) throw new Error('Failed to start run')
        return res.json()
      })
      .then(() => {
        setStatuses(prev => ({ ...prev, [configId]: 'started' }))
        setTimeout(() => setStatuses(prev => ({ ...prev, [configId]: 'idle' })), 4000)
      })
      .catch(() => setStatuses(prev => ({ ...prev, [configId]: 'idle' })))
  }

  // Helpers
  function getStatus(run: Run) {
    if (run.done || run.future?.done) return "Completed"
    if (run.future?.completedExceptionally) return "Failed"
    return "Running"
  }
  function formatDate(ts: number | null | undefined) {
    if (!ts) return "-"
    const d = new Date(ts * 1000)
    return d.toLocaleString()
  }
  function formatDuration(start: number | null | undefined, end: number | null | undefined) {
    if (!start || !end) return "-"
    const diff = Math.round(end - start)
    if (diff < 60) return `${diff}s`
    const m = Math.floor(diff / 60)
    const s = diff % 60
    return `${m}m ${s}s`
  }

  // Render logic
  if (runId && !isNewRun) {
    // Run detail view
    if (runDetailLoading) return <div className="p-8 text-center text-muted-foreground">Loading run...</div>
    if (runDetailError) return <div className="p-8 text-center text-red-500">{runDetailError}</div>
    if (!runDetail) return null
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="icon" asChild>
              <Link href="/runs">Back</Link>
            </Button>
            <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-0 text-primary-800">Run Details</h1>
          </div>
        </div>
        <Card className="border shadow-sm mb-6">
          <CardHeader className="pb-2">
            <CardTitle className="text-base sm:text-lg">Run Info</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-3">
              <div>
                <div className="text-xs text-muted-foreground mb-1">Run ID</div>
                <div className="font-mono text-sm break-all">{runDetail.runId}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Config</div>
                <div className="font-mono text-sm break-all">{runDetail.configId}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Status</div>
                <Badge variant={runDetail.done ? "success" : "secondary"} className="capitalize text-xs px-2 py-0.5">
                  {runDetail.done ? "Completed" : "In Progress"}
                </Badge>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Type</div>
                <div className="text-sm">{runDetail.runType}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Start Time</div>
                <div className="text-sm">{formatDate(runDetail.startTime)}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">End Time</div>
                <div className="text-sm">{formatDate(runDetail.endTime)}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Duration</div>
                <div className="text-sm">{formatDuration(runDetail.startTime, runDetail.endTime)}</div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (isNewRun) {
    // New run view
    if (configsLoading) return <div className="text-center py-8">Loading configurations...</div>
    if (configsError) return <div className="text-red-600 text-center py-8">{configsError}</div>
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
          <div>
            <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">Start New Run</h1>
            <p className="text-sm text-muted-foreground">Select a configuration to start a new run</p>
          </div>
          <Button variant="ghost" size="icon" asChild className="self-start">
            <Link href="/runs">Back to Runs</Link>
          </Button>
        </div>
        <Card>
          <CardHeader>
            <CardTitle>Configurations</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead className="text-right">Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {configs.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={2} className="text-center">No configurations found</TableCell>
                  </TableRow>
                ) : (
                  configs.map((cfg) => (
                    <TableRow key={cfg.id}>
                      <TableCell className="font-medium">{cfg.id}</TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="icon"
                          disabled={statuses[cfg.id] === 'loading'}
                          onClick={() => handleRun(cfg.id)}
                        >
                          {statuses[cfg.id] === 'started' ? (
                            <Check className="h-4 w-4 text-green-500" />
                          ) : (
                            <Play className="h-4 w-4" />
                          )}
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>
    )
  }

  // Default: runs list view
  if (loading) return <div className="p-8 text-center text-muted-foreground">Loading runs...</div>
  if (error) return <div className="p-8 text-center text-red-500">{error}</div>
  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
        <div>
          <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">Runs</h1>
          <p className="text-sm text-muted-foreground">Monitor and manage pipeline executions</p>
          {configIdFilter && (
            <span className="inline-block mt-2 px-2 py-1 text-xs rounded bg-blue-100 text-blue-800">Filtered by Config: <span className="font-mono">{configIdFilter}</span></span>
          )}
        </div>
        <Button className="w-full sm:w-auto bg-primary-600 hover:bg-primary-700" asChild>
          <Link href="/runs?id=new">
            <Plus className="mr-2 h-4 w-4" /> Start New Run
          </Link>
        </Button>
      </div>
      <div className="hidden md:block mb-6">
        <Card className="border shadow-sm">
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="p-3 sm:p-4 text-left">Run ID</th>
                    <th className="p-3 sm:p-4 text-left">Config</th>
                    <th className="p-3 sm:p-4 text-left">Status</th>
                    <th className="p-3 sm:p-4 text-left">Start</th>
                    <th className="p-3 sm:p-4 text-left">End</th>
                    <th className="p-3 sm:p-4 text-left">Duration</th>
                    <th className="p-3 sm:p-4 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRuns.map(run => (
                    <tr key={run.runId} className="border-b last:border-0">
                      <td className="p-3 sm:p-4 font-medium">
                        <Link href={`/runs?id=${run.runId}`}>{run.runId}</Link>
                      </td>
                      <td className="p-3 sm:p-4">{run.configId}</td>
                      <td className="p-3 sm:p-4">
                        <Badge
                          variant={getStatus(run) === "Completed"
                            ? "success"
                            : getStatus(run) === "Running"
                              ? "secondary"
                              : "destructive"}
                          className="text-[10px] px-1.5 py-0.5 capitalize"
                        >
                          {getStatus(run)}
                        </Badge>
                      </td>
                      <td className="p-3 sm:p-4">{formatDate(run.startTime)}</td>
                      <td className="p-3 sm:p-4">{formatDate(run.endTime)}</td>
                      <td className="p-3 sm:p-4">{formatDuration(run.startTime, run.endTime)}</td>
                      <td className="p-3 sm:p-4 text-right">
                        <div className="flex gap-2 justify-end">
                          <Button size="sm" variant="ghost" className="h-8 px-2 text-xs text-primary-600" asChild>
                            <Link href={`/runs?id=${run.runId}`}>View</Link>
                          </Button>
                          {getStatus(run) === "Running" && (
                            <Button size="sm" variant="destructive" className="h-8 px-2 text-xs">
                              Stop
                            </Button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      </div>
      <div className="md:hidden space-y-3 mb-6">
        {filteredRuns.map((run) => (
          <Card key={run.runId} className="border shadow-sm overflow-hidden">
            <CardHeader className="p-3 pb-0">
              <div className="flex justify-between items-start">
                <div className="space-y-1 w-2/3">
                  <Link href={`/runs?id=${run.runId}`} className="inline-block">
                    <CardTitle className="text-sm font-medium text-primary-600 hover:underline truncate">
                      {run.runId}
                    </CardTitle>
                  </Link>
                  <CardDescription className="text-xs truncate">{run.configId}</CardDescription>
                </div>
                <Badge
                  variant={getStatus(run) === "Completed"
                    ? "success"
                    : getStatus(run) === "Running"
                      ? "secondary"
                      : "destructive"}
                  className="text-[10px] px-1.5 py-0.5 capitalize"
                >
                  {getStatus(run)}
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="p-3 pt-2">
              <div className="grid grid-cols-2 gap-2 text-xs mb-3">
                <div className="flex items-center">
                  <Calendar className="mr-1.5 h-3 w-3 text-muted-foreground flex-shrink-0" />
                  <span className="truncate">{formatDate(run.startTime)}</span>
                </div>
                <div className="flex items-center">
                  <Clock className="mr-1.5 h-3 w-3 text-muted-foreground flex-shrink-0" />
                  <span>{formatDuration(run.startTime, run.endTime)}</span>
                </div>
              </div>
              <div className="flex items-center justify-between mt-1">
                <Button size="sm" variant="ghost" className="h-7 px-2 text-xs text-primary-600 -ml-2" asChild>
                  <Link href={`/runs?id=${run.runId}`}>
                    View Details <ArrowRight className="ml-1 h-3 w-3" />
                  </Link>
                </Button>
                {getStatus(run) === "Running" && (
                  <Button size="sm" variant="destructive" className="h-7 px-2 text-xs">
                    <X className="mr-1 h-3 w-3" /> Stop
                  </Button>
                )}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
      <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3">
        <div className="text-xs sm:text-sm text-muted-foreground order-2 sm:order-1 text-center sm:text-left">
          Showing {runs.length} of {runs.length} runs
        </div>
      </div>
    </div>
  )
}
