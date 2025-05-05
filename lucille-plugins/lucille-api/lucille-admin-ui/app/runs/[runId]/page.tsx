import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { getRunById, getConfigById } from "@/lib/api";
import { notFound } from "next/navigation";
import { ArrowLeft } from "lucide-react";
import NewRunPageClient from './NewRunPageClient';

// Using the proper Next.js 15 interface for dynamic params
interface PageProps {
  // Next.js 15.x dynamic params must be awaited before accessing properties
  params: Promise<{ runId: string }>;
}

// Reference: https://nextjs.org/docs/messages/sync-dynamic-apis
export default async function RunDetailPage(props: PageProps) {
  // Await the params promise to extract runId
  const { runId } = await props.params;
  
  // New run layout when runId is 'new'
  if (runId === 'new') {
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
          <div>
            <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">
              Start New Run
            </h1>
            <p className="text-sm text-muted-foreground">
              Select a configuration to start a new run
            </p>
          </div>
          <Button variant="ghost" size="icon" asChild className="self-start">
            <Link href="/runs">
              <ArrowLeft className="h-5 w-5" /> Back to Runs
            </Link>
          </Button>
        </div>
        <Card>
          <CardHeader>
            <CardTitle>Configurations</CardTitle>
          </CardHeader>
          <CardContent>
            <NewRunPageClient />
          </CardContent>
        </Card>
      </div>
    );
  }
  
  // Use the getRunById function which has been fixed in lib/api.ts
  const run = await getRunById(runId);
  
  if (!run) {
    return notFound();
  }

  try {
    const config = await getConfigById(run.configId);

    // Format dates for display
    const startTime = run.startTime ? new Date(run.startTime * 1000) : null;
    const endTime = run.endTime ? new Date(run.endTime * 1000) : null;
    const formattedStart = startTime ? startTime.toLocaleString() : "N/A";
    const formattedEnd = endTime ? endTime.toLocaleString() : (run.done ? "N/A" : "In progress");
    const duration = startTime && endTime ? Math.round((endTime.getTime() - startTime.getTime())/1000) : null;

    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex items-center gap-4 mb-6">
          <Button variant="ghost" size="icon" asChild className="self-start">
            <Link href="/runs">
              <ArrowLeft className="h-5 w-5" />
            </Link>
          </Button>
          <h1 className="text-2xl font-bold">Run Details</h1>
        </div>
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Run Info</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <div><strong>Run ID:</strong> {run.runId}</div>
              <div><strong>Config:</strong> {config ? (
                <Link href={`/configs/${config.id}`} className="underline text-primary">{config.name}</Link>
              ) : run.configId}</div>
              <div><strong>Status:</strong> {run.done ? "Completed" : "In Progress"}</div>
              <div><strong>Start Time:</strong> {formattedStart}</div>
              <div><strong>End Time:</strong> {formattedEnd}</div>
              <div><strong>Duration:</strong> {duration !== null ? `${duration} seconds` : "-"}</div>
              <div><strong>Type:</strong> {run.runType}</div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  } catch (error) {
    console.error("Error in RunDetailPage:", error);
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex items-center gap-4 mb-6">
          <Button variant="ghost" size="icon" asChild className="self-start">
            <Link href="/runs">
              <ArrowLeft className="h-5 w-5" />
            </Link>
          </Button>
          <h1 className="text-2xl font-bold">Error</h1>
        </div>
        <Card className="mb-6">
          <CardHeader>
            <CardTitle>Error Loading Run Details</CardTitle>
          </CardHeader>
          <CardContent>
            <p>There was a problem loading the run details. Please try again later.</p>
            <Button className="mt-4" asChild>
              <Link href="/runs">Back to Runs List</Link>
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }
}
