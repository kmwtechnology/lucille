"use client";

import { Disclosure } from "@headlessui/react";
import { ChevronRight, ChevronDown, Copy, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/components/ui/use-toast";
import { useState } from "react";

interface JsonViewerClientProps {
  data: any;
  name?: string;
  isRoot?: boolean;
  onCopy?: (value: string) => void;
}

export function JsonViewerClient({ 
  data, 
  name, 
  isRoot = true,
  onCopy 
}: JsonViewerClientProps) {
  const { toast } = useToast();
  const [searchTerm, setSearchTerm] = useState("");

  const handleCopy = (value: string) => {
    if (onCopy) {
      onCopy(value);
      return;
    }
    
    navigator.clipboard.writeText(value);
    toast({
      title: "Copied to clipboard",
      description: "The value has been copied to your clipboard",
    });
  };

  const matchesSearch = (value: any): boolean => {
    if (typeof value === 'string') {
      return value.toLowerCase().includes(searchTerm.toLowerCase());
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
      return String(value).includes(searchTerm);
    }
    if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
      return JSON.stringify(value).toLowerCase().includes(searchTerm.toLowerCase());
    }
    return false;
  };

  const renderValue = (value: any) => {
    if (typeof value === 'string') {
      return (
        <span className="text-foreground">
          "{value}"
          <Button
            variant="ghost"
            size="icon"
            className="ml-1 p-1 hover:bg-primary-50"
            onClick={() => handleCopy(value)}
          >
            <Copy className="h-4 w-4" />
          </Button>
        </span>
      );
    }
    if (typeof value === 'number') {
      return (
        <span className="text-foreground">
          {value}
          <Button
            variant="ghost"
            size="icon"
            className="ml-1 p-1 hover:bg-primary-50"
            onClick={() => handleCopy(String(value))}
          >
            <Copy className="h-4 w-4" />
          </Button>
        </span>
      );
    }
    if (typeof value === 'boolean') {
      return (
        <span className="text-foreground">
          {value ? "true" : "false"}
          <Button
            variant="ghost"
            size="icon"
            className="ml-1 p-1 hover:bg-primary-50"
            onClick={() => handleCopy(String(value))}
          >
            <Copy className="h-4 w-4" />
          </Button>
        </span>
      );
    }
    return String(value);
  };

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
            <JsonViewerClient data={item} />
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
                  <JsonViewerClient data={value} />
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
