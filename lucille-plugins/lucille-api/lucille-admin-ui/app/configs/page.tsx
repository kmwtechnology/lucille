"use client";

import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {

} from "@/components/ui/table";
import { Plus, Eye } from "lucide-react";
import { useEffect, useState } from "react";

function ConfigsTable() {
  const [configs, setConfigs] = useState<{ id: string }[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(false);
    fetch("http://localhost:8080/v1/config")
      .then((res) => {
        if (!res.ok) throw new Error("Bad status");
        return res.json();
      })
      .then((data) => {
        setConfigs(Object.keys(data || {}).map((id) => ({ id })));
      })
      .catch(() => {
        setError(true);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="p-8 text-center text-muted-foreground">Loading configurations...</div>
    );
  }
  if (error) {
    return <div className="p-8 text-center text-red-500">Lucille Unavailable</div>;
  }

  return (
    <>
      <div className="hidden md:block mb-6">
        <Card className="border shadow-sm">
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="p-3 sm:p-4 text-left">Config ID</th>
                    <th className="p-3 sm:p-4 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {configs.map((config) => (
                    <tr key={config.id} className="border-b last:border-0">
                      <td className="p-3 sm:p-4 font-medium">
                        <Link href={`/configs/detail?id=${config.id}`}>{config.id}</Link>
                      </td>
                      <td className="p-3 sm:p-4 text-right">
                        <Button variant="ghost" size="icon" asChild>
                          <Link href={`/configs/detail?id=${config.id}`} aria-label="View config">
                            <Eye className="h-4 w-4" />
                          </Link>
                        </Button>
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
        {configs.map((config) => (
          <Card key={config.id} className="border shadow-sm overflow-hidden">
            <CardHeader className="p-3 pb-0">
              <div className="flex justify-between items-start">
                <div className="space-y-1 w-2/3">
                  <Link href={`/configs/detail?id=${config.id}`} className="inline-block">
                    <CardTitle className="text-sm font-medium text-primary-600 hover:underline truncate">
                      {config.id}
                    </CardTitle>
                  </Link>
                </div>
                <Button variant="ghost" size="icon" asChild>
                  <Link href={`/configs/detail?id=${config.id}`} aria-label="View config">
                    <Eye className="h-4 w-4" />
                  </Link>
                </Button>
              </div>
            </CardHeader>
          </Card>
        ))}
      </div>
      <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3">
        <div className="text-xs sm:text-sm text-muted-foreground order-2 sm:order-1 text-center sm:text-left">
          Showing {configs.length} of {configs.length} configurations
        </div>
      </div>
    </>
  );
}

export default function ConfigsPage() {
  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-6">
        <div>
          <h1 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-1 sm:mb-2 text-primary-800">Configurations</h1>
          <p className="text-sm text-muted-foreground">Manage your pipeline configurations</p>
        </div>
        <Button className="w-full sm:w-auto bg-primary-600 hover:bg-primary-700" asChild>
          <Link href="/configs/detail?id=new">
            <Plus className="mr-2 h-4 w-4" /> New Configuration
          </Link>
        </Button>
      </div>
      <ConfigsTable />
    </div>
  );
}
