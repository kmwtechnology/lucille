"use client";

import { Disclosure } from "@headlessui/react";
import { ChevronRight, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface JsonViewerProps {
  data: any;
  name?: string;
  isRoot?: boolean;
}

export function JsonViewer({ data, name, isRoot = true }: JsonViewerProps) {
  if (data === null) return <span className="text-muted-foreground">null</span>;
  if (data === undefined) return <span className="text-muted-foreground">undefined</span>;

  if (Array.isArray(data)) {
    return (
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground">[</span>
          <span className="text-sm text-muted-foreground">{data.length} items</span>
        </div>
        {data.map((item, index) => (
          <div key={index} className="ml-4">
            <JsonViewer data={item} />
          </div>
        ))}
        <span className="text-muted-foreground">]</span>
      </div>
    );
  }

  if (typeof data === 'object' && data !== null) {
    return (
      <Disclosure as="div" className="space-y-2">
        {({ open }) => (
          <>
            <Disclosure.Button className="flex w-full items-center justify-between">
              <span className="text-muted-foreground">{open ? "{" : "}"}</span>
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">{Object.keys(data).length} properties</span>
                {open ? (
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-muted-foreground" />
                )}
              </div>
            </Disclosure.Button>
            <Disclosure.Panel className="ml-4">
              {Object.entries(data).map(([key, value]) => (
                <div key={key} className="flex flex-col gap-1">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-muted-foreground">{key}</span>
                    <span className="text-muted-foreground">:</span>
                  </div>
                  <JsonViewer data={value} />
                </div>
              ))}
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    );
  }

  // Handle primitive values
  if (typeof data === 'string') {
    return <span className="text-foreground">"{data}"</span>;
  }
  if (typeof data === 'number') {
    return <span className="text-foreground">{data}</span>;
  }
  if (typeof data === 'boolean') {
    return <span className="text-foreground">{data ? "true" : "false"}</span>;
  }

  // Fallback
  return <span className="text-muted-foreground">{String(data)}</span>;
}

  if (data === null) return <span className="text-muted-foreground">null</span>;
  if (data === undefined) return <span className="text-muted-foreground">undefined</span>;

  if (Array.isArray(data)) {
    const filteredItems = searchTerm
      ? data.filter((item) => matchesSearch(item))
      : data;

    return (
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground">[</span>
          <span className="text-sm text-muted-foreground">
            {filteredItems.length} of {data.length} items
          </span>
        </div>
        {filteredItems.map((item, index) => (
          <div key={index} className="ml-4">
            <JsonViewer data={item} />
          </div>
        ))}
        <span className="text-muted-foreground">]</span>
      </div>
    );
  }

  if (typeof data === 'object' && data !== null) {
    const entries = searchTerm
      ? Object.entries(data).filter(([key, value]) =>
          key.toLowerCase().includes(searchTerm.toLowerCase()) || matchesSearch(value)
        )
      : Object.entries(data);

    return (
      <Disclosure as="div" className="space-y-2">
        {({ open }) => (
          <>
            <Disclosure.Button className="flex w-full items-center justify-between">
              <span className="text-muted-foreground">{open ? "{" : "}"}</span>
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">
                  {entries.length} of {Object.keys(data).length} properties
                </span>
                {open ? (
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-muted-foreground" />
                )}
              </div>
            </Disclosure.Button>
            <Disclosure.Panel className="ml-4">
              {isRoot && (
                <div className="mb-4">
                  <Input
                    placeholder="Search..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="w-full"
                  />
                </div>
              )}
              {entries.map(([key, value]) => (
                <div key={key} className="flex flex-col gap-1">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-muted-foreground">{key}</span>
                    <span className="text-muted-foreground">:</span>
                  </div>
                  <JsonViewer data={value} />
                </div>
              ))}
            </Disclosure.Panel>
          </>
        )}
      </Disclosure>
    );
  }

  return renderValue(data);
}
