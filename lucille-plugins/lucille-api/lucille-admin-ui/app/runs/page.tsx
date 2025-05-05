"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Plus, Search, ArrowRight, Clock, Calendar, X, Filter, SortAsc } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { useEffect, useState } from "react"

// Type for a pipeline run from the API
interface Run {
  runId: string;
  configId: string;
  startTime: number | null;
  endTime: number | null;
  runResult: unknown;
  runType: string;
  done: boolean;
  future?: {
    done?: boolean;
    cancelled?: boolean;
    completedExceptionally?: boolean;
    numberOfDependents?: number;
  };
}

export default function RunsPage() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function fetchRuns() {
      setLoading(true);
      setError("");
      try {
        const res = await fetch("/api/lucille/run");
        if (!res.ok) throw new Error("Lucille Unavailable");
        const data = await res.json();
        setRuns(Array.isArray(data) ? data : []);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    }
    fetchRuns();
  }, []);

  // Helper to format status, time, duration
  function getStatus(run: Run) {
    if (run.done || run.future?.done) return "Completed";
    if (run.future?.completedExceptionally) return "Failed";
    return "Running";
  }
  function formatDate(ts: number | null | undefined) {
    if (!ts) return "-";
    const d = new Date(ts * 1000);
    return d.toLocaleString();
  }
  function formatDuration(start: number | null | undefined, end: number | null | undefined) {
    if (!start || !end) return "-";
    const diff = Math.round(end - start);
    if (diff < 60) return `${diff}s`;
    const m = Math.floor(diff / 60);
    const s = diff % 60;
    return `${m}m ${s}s`;
  }

  if (loading) {
    return <div className="p-8 text-center text-muted-foreground">Loading runs...</div>;
  }
  if (error) {
    return <div className="p-8 text-center text-red-500">{error}</div>;
  }

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      {/* Header section */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
        <div>
          <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">Runs</h1>
          <p className="text-sm text-muted-foreground">Monitor and manage pipeline executions</p>
        </div>
        <Button className="w-full sm:w-auto bg-primary-600 hover:bg-primary-700" asChild>
          <Link href="/runs/new">
            <Plus className="mr-2 h-4 w-4" /> Start New Run
          </Link>
        </Button>
      </div>

      {/* Search and filter section */}
      <div className="flex flex-col sm:flex-row gap-3 mb-6">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Search runs..." className="pl-10 text-sm" />
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" className="flex-1 sm:flex-none text-xs sm:text-sm">
            <Filter className="mr-1.5 h-3.5 w-3.5" /> Filter
          </Button>
          <Button variant="outline" size="sm" className="flex-1 sm:flex-none text-xs sm:text-sm">
            <SortAsc className="mr-1.5 h-3.5 w-3.5" /> Sort
          </Button>
        </div>
      </div>

      {/* Desktop view - Table (hidden on mobile) */}
      <div className="hidden md:block mb-6">
        <Card className="border shadow-sm">
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Run ID</th>
                    <th className="text-left p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Configuration</th>
                    <th className="text-left p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Status</th>
                    <th className="text-left p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Start Time</th>
                    <th className="text-left p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Duration</th>
                    <th className="text-right p-3 sm:p-4 font-medium text-primary-800 text-xs sm:text-sm">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr key={run.runId} className="border-b last:border-0">
                      <td className="p-3 sm:p-4 text-xs sm:text-sm font-mono">{run.runId}</td>
                      <td className="p-3 sm:p-4 text-xs sm:text-sm">{run.configId}</td>
                      <td className="p-3 sm:p-4 text-xs sm:text-sm">
                        <Badge
                          variant={getStatus(run) === "Completed"
                            ? "success"
                            : getStatus(run) === "Running"
                              ? "secondary"
                              : "destructive"}
                          className="capitalize"
                        >
                          {getStatus(run)}
                        </Badge>
                      </td>
                      <td className="p-3 sm:p-4 text-xs sm:text-sm">{formatDate(run.startTime)}</td>
                      <td className="p-3 sm:p-4 text-xs sm:text-sm">{formatDuration(run.startTime, run.endTime)}</td>
                      <td className="p-3 sm:p-4 text-right">
                        <div className="flex gap-2 justify-end">
                          <Button size="sm" variant="ghost" className="h-8 px-2 text-xs text-primary-600" asChild>
                            <Link href={`/runs/${run.runId}`}>View</Link>
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

      {/* Mobile view - Cards (hidden on desktop) */}
      <div className="md:hidden space-y-3 mb-6">
        {runs.map((run) => (
          <Card key={run.runId} className="border shadow-sm overflow-hidden">
            <CardHeader className="p-3 pb-0">
              <div className="flex justify-between items-start">
                <div className="space-y-1 w-2/3">
                  <Link href={`/runs/${run.runId}`} className="inline-block">
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
                  <Link href={`/runs/${run.runId}`}>
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

      {/* Pagination */}
      <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3">
        <div className="text-xs sm:text-sm text-muted-foreground order-2 sm:order-1 text-center sm:text-left">
          Showing {runs.length} of {runs.length} runs
        </div>
        <div className="flex justify-center gap-2 order-1 sm:order-2">
          <Button variant="outline" size="sm" className="h-8 text-xs" disabled>
            Previous
          </Button>
          <Button variant="outline" size="sm" className="h-8 text-xs text-primary-600">
            Next
          </Button>
        </div>
      </div>
    </div>
  )
}
