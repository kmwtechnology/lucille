"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { JsonViewerClient } from "@/components/ui/json-viewer-client";
import { Loader2 } from "lucide-react";
import { useEffect, useState } from "react";
import { JavadocModal } from "@/components/ui/javadoc-modal";

export default function IndexersPage() {
  const [indexers, setIndexers] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    
    fetch("http://localhost:8080/v1/config-info/indexer-list")
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch indexer list");
        return res.json();
      })
      .then((data) => {
        setIndexers(data);
      })
      .catch((err) => {
        setError(err.message);
      })
      .finally(() => {
        setLoading(false);
      });
  }, []);

  if (loading) {
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex flex-col items-center justify-center min-h-[200px]">
          <Loader2 className="h-8 w-8 animate-spin text-primary-600" />
          <p className="mt-2 text-sm text-muted-foreground">Loading indexers...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 sm:p-6 lg:p-8">
        <div className="flex flex-col items-center justify-center min-h-[200px]">
          <p className="text-sm text-red-500">Error loading indexers: {error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
        <div>
          <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">
            Indexers
          </h1>
          <p className="text-sm text-muted-foreground">
            Available indexer configurations
          </p>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {indexers.map((indexer, index) => (
          <Card key={index} className="border-primary-100 shadow-sm hover:shadow-md transition-shadow">
            <CardHeader className="pb-2">
              <div className="space-y-2">
                <div className="flex justify-end">
                  <JavadocModal className={indexer.className} type="indexer" />
                </div>
                <div>
                  <CardTitle className="text-base sm:text-lg break-words truncate max-w-full">
                    {indexer.className ? indexer.className.split('.').pop() : `Indexer ${index + 1}`}
                  </CardTitle>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-muted-foreground">Properties</h3>
                  <div className="space-y-2">
                    {['packageName', 'superClassName'].map((key) => {
  const value = indexer[key];
  if (!value) return null;
  return (
    <div key={key} className="flex flex-col gap-1">
      <div className="text-sm font-medium text-muted-foreground">{key}</div>
      <div className="text-xs text-muted-foreground">{value}</div>
    </div>
  );
})}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
