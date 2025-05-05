"use client"

import { useEffect } from "react"
import { Button } from "@/components/ui/button"
import { AlertCircle, RefreshCcw } from "lucide-react"

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    // Log the error to an error reporting service
    console.error(error)
  }, [error])

  return (
    <div className="p-8 flex flex-col items-center justify-center min-h-[50vh]">
      <div className="flex flex-col items-center text-center max-w-md">
        <div className="rounded-full bg-red-100 p-3 mb-4">
          <AlertCircle className="h-6 w-6 text-red-600" />
        </div>
        <h2 className="text-2xl font-bold mb-2">Something went wrong</h2>
        <p className="text-muted-foreground mb-6">
          We encountered an error while loading the configurations. Please try again or contact support if the problem
          persists.
        </p>
        <Button onClick={reset} className="flex items-center gap-2">
          <RefreshCcw className="h-4 w-4" />
          Try again
        </Button>
      </div>
    </div>
  )
}
