"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { ArrowRight, Database, Play, Settings, Activity } from "lucide-react"
import { useEffect, useState } from "react"

export default function Dashboard() {
  const [status, setStatus] = useState<'loading'|'healthy'|'unavailable'|'notready'>('loading');
  const [totalConfigs, setTotalConfigs] = useState<number|null>(null);
  const [configsLoading, setConfigsLoading] = useState<boolean>(true);
  const [configsError, setConfigsError] = useState<string|null>(null);
  // Pipeline runs stats
  const [runs, setRuns] = useState<any[] | null>(null);
  const [runsLoading, setRunsLoading] = useState<boolean>(true);
  // System stats
  const [sysStats, setSysStats] = useState<any>(null);
  const [sysStatsLoading, setSysStatsLoading] = useState<boolean>(true);

  useEffect(() => {
    let _isActive = true;
    async function checkHealth() {
      setStatus('loading');
      // First: liveness
      try {
        const res = await fetch('/api/lucille/livez');
        const data = await res.json();
        if (!res.ok || !data.ok) {
          setStatus('unavailable');
          return;
        }
      } catch {
        setStatus('unavailable');
        return;
      }
      // Second: readiness
      try {
        const res = await fetch('/api/lucille/readyz');
        const data = await res.json();
        if (res.ok && data.ok) {
          setStatus('healthy');
        } else {
          setStatus('notready');
        }
      } catch {
        setStatus('notready');
      }
    }
    checkHealth();
    return () => { _isActive = false; };
  }, []);

  useEffect(() => {
    let _isActive = true;
    async function fetchConfigs() {
      setConfigsLoading(true);
      setConfigsError(null);
      try {
        const res = await fetch('/api/lucille/config');
        if (!res.ok) throw new Error('Failed to fetch configs');
        const data = await res.json();
        let count = null;
        if (data && typeof data === 'object' && !Array.isArray(data)) {
          count = Object.keys(data).length;
        }
        if (count === null) {
          // For debugging: log to console
          // eslint-disable-next-line no-console
          console.error('Unexpected configs response:', data);
          throw new Error('Malformed configs response');
        }
        if (_isActive) setTotalConfigs(count);
      } catch (error) {
        if (_isActive) setConfigsError(error instanceof Error ? error.message : 'Unknown error');
      } finally {
        if (_isActive) setConfigsLoading(false);
      }
    }
    fetchConfigs();
    return () => { _isActive = false; };
  }, []);

  // Fetch system stats
  useEffect(() => {
    let active = true;
    async function fetchStats() {
      try {
        const res = await fetch('/api/lucille/systemstats');
        const data = await res.json();
        if (active) setSysStats(data);
      } catch {
        console.warn('Failed to fetch system stats');
      } finally {
        if (active) setSysStatsLoading(false);
      }
    }
    fetchStats();
    return () => { active = false; };
  }, []);

  // Fetch runs on mount
  useEffect(() => {
    let _isActive = true;
    async function fetchRuns() {
      setRunsLoading(true);
      try {
        const res = await fetch('/api/lucille/run');
        const data = await res.json();
        if (_isActive) setRuns(Array.isArray(data) ? data : []);
      } catch (e) {
        console.warn('Failed to fetch runs', e);
      } finally {
        if (_isActive) setRunsLoading(false);
      }
    }
    fetchRuns();
    return () => { _isActive = false; };
  }, []);

  // Compute counts
  const todaysRunsCount = runs?.length ?? 0;
  const completedCount = runs?.filter(r => r.done).length ?? 0;
  const runningCount = runs?.filter(r => !r.done).length ?? 0;

  // Helper to format relative time
  function timeAgo(unixSec: number): string {
    const now = Date.now() / 1000;
    const diff = now - unixSec;
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff/3600)}h ago`;
    return `${Math.floor(diff/86400)}d ago`;
  }

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="mb-6 sm:mb-8">
        <h1 className="text-2xl sm:text-3xl font-bold mb-2 text-primary-800">Lucille Admin Dashboard</h1>
        <p className="text-muted-foreground">Manage your data pipelines and configurations</p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6 mb-6 sm:mb-8">
        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full">
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className="flex items-center gap-2 text-base sm:text-lg">
              <Activity className={`h-5 w-5 flex-shrink-0 ${status === 'unavailable' ? 'text-red-600' : status === 'notready' ? 'text-yellow-600' : 'text-primary-600'}`}/>
              <span className="truncate">System Status</span>
            </CardTitle>
            <CardDescription className="text-xs sm:text-sm">Current system health and metrics</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">API Status</span>
                {status === 'loading' && (
                  <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-teal-100 text-teal-800">Checking...</span>
                )}
                {status === 'healthy' && (
                  <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-teal-100 text-teal-800">Healthy</span>
                )}
                {status === 'notready' && (
                  <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-yellow-100 text-yellow-800">Not Ready</span>
                )}
                {status === 'unavailable' && (
                  <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800">Lucille Unavailable</span>
                )}
              </div>
              {/* System resource percentages */}
              {!sysStatsLoading && sysStats && sysStats.jvm && sysStats.cpu && sysStats.ram && sysStats.storage ? (
                <>
                  <div className="flex justify-between items-center">
                    <span className="text-xs sm:text-sm">JVM</span>
                    <span className="font-medium text-xs sm:text-sm">{sysStats.jvm.percent.toFixed(0)}%</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-xs sm:text-sm">CPU</span>
                    <span className="font-medium text-xs sm:text-sm">{sysStats.cpu.percent.toFixed(0)}%</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-xs sm:text-sm">Memory</span>
                    <span className="font-medium text-xs sm:text-sm">{sysStats.ram.percent.toFixed(0)}%</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-xs sm:text-sm">Storage</span>
                    <span className="font-medium text-xs sm:text-sm">{sysStats.storage.percent.toFixed(0)}%</span>
                  </div>
                </>
              ) : (!sysStatsLoading && <div className="text-sm text-muted-foreground">Please check your Lucille instance</div>)}
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="ghost"
              size="sm"
              className="w-full text-primary-600 hover:text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/health">
                View Details <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
              </Link>
            </Button>
          </CardFooter>
        </Card>

        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full">
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className="flex items-center gap-2 text-base sm:text-lg">
              <Settings className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Configurations</span>
            </CardTitle>
            <CardDescription className="text-xs sm:text-sm">Manage your pipeline configurations</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Total Configs</span>
                {configsLoading ? (
                  <span className="text-xs font-medium text-muted-foreground">...</span>
                ) : configsError ? (
                  <span className="text-xs font-medium text-red-600">Lucille Unavailable</span>
                ) : (
                  <span className="font-medium text-xs sm:text-sm">{totalConfigs}</span>
                )}
              </div>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="ghost"
              size="sm"
              className="w-full text-primary-600 hover:text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/configs">
                Manage Configs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
              </Link>
            </Button>
          </CardFooter>
        </Card>

        <Card className="border-primary-100 shadow-sm hover:shadow-md transition-shadow flex flex-col h-full">
          <CardHeader className="pb-2 space-y-1">
            <CardTitle className="flex items-center gap-2 text-base sm:text-lg">
              <Play className="h-5 w-5 text-primary-600 flex-shrink-0" />
              <span className="truncate">Pipeline Runs</span>
            </CardTitle>
            <CardDescription className="text-xs sm:text-sm">Monitor pipeline executions</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Today's Runs</span>
                {runsLoading ? (
                  <span className="text-xs sm:text-sm">Loading...</span>
                ) : runs ? (
                  <span className="text-xs sm:text-sm">{todaysRunsCount}</span>
                ) : (
                  <span className="text-xs font-medium text-red-600">Lucille Unavailable</span>
                )}
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Completed</span>
                <span className="font-medium text-xs sm:text-sm">{runsLoading ? '' : runs ? completedCount : ''}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs sm:text-sm">Running</span>
                <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-800">
                  {runsLoading ? '' : runs ? `${runningCount} Active` : ''}
                </span>
              </div>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="ghost"
              size="sm"
              className="w-full text-primary-600 hover:text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/runs">
                View Runs <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
              </Link>
            </Button>
          </CardFooter>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
        <Card className="border-primary-100 shadow-sm flex flex-col h-full">
          <CardHeader>
            <CardTitle className="text-lg sm:text-xl text-primary-800">Recent Runs</CardTitle>
            <CardDescription className="text-xs sm:text-sm">Latest pipeline executions</CardDescription>
          </CardHeader>
          <CardContent className="flex-1">
            <div className="space-y-3 sm:space-y-4">
              {runsLoading ? (
                <div>Loading...</div>
              ) : runs && runs.length > 0 ? (
                runs.map(run => (
                  <div key={run.runId} className="flex items-center justify-between border-b pb-3 last:border-0 last:pb-0">
                    <div>
                      <div className="font-medium text-sm sm:text-base">Run ID: {run.runId}</div>
                      <div className="text-xs sm:text-sm text-muted-foreground">Config ID: {run.configId}</div>
                      <div className="text-xs sm:text-sm text-muted-foreground">{timeAgo(run.startTime)}</div>
                    </div>
                    <div>
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${run.done ? "bg-green-100 text-green-800" : "bg-secondary-100 text-secondary-800"}`}>
                        {run.done ? "COMPLETED" : "RUNNING"}
                      </span>
                    </div>
                  </div>
                ))
              ) : (
                <div className="text-sm text-muted-foreground">No runs found</div>
              )}
            </div>
          </CardContent>
          <CardFooter>
            <Button
              variant="outline"
              size="sm"
              className="w-full border-primary-200 text-primary-700 hover:bg-primary-50 text-xs sm:text-sm py-1.5"
              asChild
            >
              <Link href="/runs">View All Runs</Link>
            </Button>
          </CardFooter>
        </Card>

        <Card className="border-primary-100 shadow-sm flex flex-col h-full">
          <CardHeader>
            <CardTitle className="text-lg sm:text-xl text-primary-800">Quick Actions</CardTitle>
            <CardDescription className="text-xs sm:text-sm">Common operations</CardDescription>
          </CardHeader>
          <CardContent className="flex-1 grid gap-3 sm:gap-4">
            <Button className="w-full justify-start bg-primary-600 hover:bg-primary-700 text-xs sm:text-sm py-2 h-auto" asChild>
              <Link href="/configs/new">
                <Settings className="mr-2 h-4 w-4 flex-shrink-0" /> Create New Configuration
              </Link>
            </Button>
            <Button className="w-full justify-start bg-secondary-600 hover:bg-secondary-700 text-xs sm:text-sm py-2 h-auto" asChild>
              <Link href="/runs/new">
                <Play className="mr-2 h-4 w-4 flex-shrink-0" /> Start New Run
              </Link>
            </Button>
            <Button className="w-full justify-start text-xs sm:text-sm py-2 h-auto" variant="outline" asChild>
              <Link href="/configs/templates">
                <Database className="mr-2 h-4 w-4 flex-shrink-0" /> Browse Configuration Templates
              </Link>
            </Button>
            <Button className="w-full justify-start text-xs sm:text-sm py-2 h-auto" variant="outline" asChild>
              <Link href="/health">
                <Activity className="mr-2 h-4 w-4 flex-shrink-0" /> Check System Health
              </Link>
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
