"use client";

import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { useState } from "react";
import { ChevronDown } from "lucide-react";

interface JavadocModalProps {
  className: string;
  type?: 'connector' | 'indexer' | 'stage';
}

export function JavadocModal({ className, type = 'connector' }: JavadocModalProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [javadoc, setJavadoc] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchJavadoc = async (className: string) => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`http://localhost:8080/v1/config-info/javadoc-list/${type}`);
      if (!response.ok) {
        throw new Error('Failed to fetch JavaDocs');
      }
      const data = await response.json();
      
      // Find the javadoc for the specific class
      const javadocEntry = data.find((entry: any) => entry.className === className);
      if (!javadocEntry) {
        setError('No JavaDocs found for this class');
        return;
      }

      // Helper to extract config parameters from description
      function extractConfigParams(desc: string) {
        // Try to find config parameter section (case-insensitive, supports 'Config Parameters' or 'Config Parameters -')
        const configMatch = desc.match(/Config Parameters[\s\-:]*([\s\S]+)/i);
        if (!configMatch) return null;
        // Split by lines, filter out empty, remove HTML tags if any
        const paramLines = configMatch[1]
          .split(/<br>|\n/)
          .map(l => l.replace(/<[^>]+>/g, '').trim())
          .filter(l => l.length > 0 && !/^s3|azure|gcp/i.test(l)); // skip cloud storage blocks
        // Group lines that look like param: desc
        const params = paramLines
          .map(l => {
            const m = l.match(/^([\w\-\[\]\.]+)\s*\(([^)]*)\)\s*:\s*(.+)$/) || l.match(/^([\w\-\[\]\.]+)\s*:\s*(.+)$/);
            if (m) {
              return { name: m[1], type: m[2] || '', desc: m[3] || '' };
            }
            return null;
          })
          .filter(Boolean);
        return params.length ? params : null;
      }

      // Format config params if found
      let configParamsBlock = '';
      if (javadocEntry.description) {
        const params = extractConfigParams(javadocEntry.description);
        if (params) {
          configParamsBlock = `
            <div class="space-y-2">
              <h2 class="text-lg font-semibold">Config Parameters</h2>
              <table class="table-auto text-xs border border-gray-200">
                <thead><tr><th class="px-2 py-1">Name</th><th class="px-2 py-1">Type</th><th class="px-2 py-1">Description</th></tr></thead>
                <tbody>
                  ${params.filter((p): p is { name: string; type: string; desc: string } => p !== null && typeof p === 'object').map(p => `
                    <tr>
                      <td class="border px-2 py-1 font-mono">${p.name}</td>
                      <td class="border px-2 py-1">${p.type}</td>
                      <td class="border px-2 py-1">${p.desc}</td>
                    </tr>
                  `).join('')}

                </tbody>
              </table>
            </div>
          `;
        }
      }

      // Format the javadoc data
      const formattedJavadoc = `
        <div class="space-y-6">
          <!-- Class Info Section -->
          <div class="bg-muted/40 rounded-lg p-4 border border-muted-200 shadow-sm">
            <h2 class="text-xl font-bold mb-3 tracking-tight flex items-center gap-2">ℹ️ Class Information
              <button title="Copy class name" onclick="navigator.clipboard.writeText('${javadocEntry.className}')" class="ml-2 text-xs px-2 py-0.5 rounded bg-gray-100 hover:bg-gray-200 border border-gray-200 cursor-pointer">Copy</button>
            </h2>
            
            <div class="space-y-2">
              <div class="flex flex-col gap-1">
                <span class="text-sm font-medium text-muted-foreground">Class Name</span>
                <span class="text-xs font-mono text-primary-900">${javadocEntry.className}</span>
              </div>
              <div class="flex flex-col gap-1">
                <span class="text-sm font-medium text-muted-foreground">packageName</span>
                <span class="text-xs font-mono text-muted-foreground">${javadocEntry.packageName}</span>
              </div>
              
              <div class="flex flex-row gap-4 mt-2">
                <div class="flex items-center gap-2">
                  <span class="text-sm font-medium text-muted-foreground" title="Is this class abstract?">Abstract</span>
                  <span class="inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${javadocEntry.isAbstract ? 'bg-blue-100 text-blue-800' : 'bg-gray-100 text-gray-700'}">${javadocEntry.isAbstract ? 'Yes' : 'No'}</span>
                </div>
                ${typeof javadocEntry.isConfigClass === 'boolean' ? `
                <div class="flex items-center gap-2">
                  <span class="text-sm font-medium text-muted-foreground" title="A config class can be used directly in pipeline configuration">Config Class</span>
                  <span class="inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${javadocEntry.isConfigClass ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-700'}">${javadocEntry.isConfigClass ? 'Yes' : 'No'}</span>
                </div>
                ` : ''}
              </div>
            </div>
          </div>

          

          <!-- Description Section -->
          ${javadocEntry.description ? `
            <div class="bg-white/80 rounded-lg p-4 border border-muted-200 shadow-sm">
              <h2 class="text-lg font-semibold mb-2 tracking-tight flex items-center gap-2">📝 Description</h2>
              <div class="prose max-w-none text-sm">
                ${javadocEntry.description.replace(/\n/g, '<br>')}
              </div>
            </div>
          ` : ''}

          

          <!-- Config Parameters Section -->
          ${configParamsBlock ? `<div class=\"bg-white/80 rounded-lg p-4 border border-muted-200 shadow-sm\"><h2 class=\"text-lg font-semibold mb-2 tracking-tight flex items-center gap-2\">⚙️ Config Parameters</h2>${configParamsBlock}</div>` : ''}

          <!-- Methods Section -->
          ${javadocEntry.methods && javadocEntry.methods.length > 0 ? `
            <div class="bg-muted/40 rounded-lg p-4 border border-muted-200 shadow-sm">
              <h2 class="text-lg font-semibold mb-2 tracking-tight flex items-center gap-2">🔧 Methods</h2>
              <div class="grid gap-4">
                ${javadocEntry.methods.map((method: any) => {
                  const signature = `${method.methodName}(${(method.parameterNames||[]).join(', ')})`;
                  return `
                  <div class="bg-white rounded-md p-3 border border-muted-200 hover:shadow-md transition-shadow group">
                    <div class="flex items-center gap-2 mb-1">
                      <span class="text-base font-mono font-semibold text-primary-800">${method.methodName}</span>
                      ${method.returnType ? `<span class=\"ml-2 text-xs bg-blue-100 text-blue-800 px-2 py-0.5 rounded\">${method.returnType}</span>` : ''}
                      <button title="Copy method signature" onclick=\"navigator.clipboard.writeText('${signature}')\" class=\"ml-2 text-xs px-2 py-0.5 rounded bg-gray-100 hover:bg-gray-200 border border-gray-200 cursor-pointer hidden group-hover:inline-block\">Copy</button>
                    </div>
                    <div class="space-y-1 ml-2">
                      ${(method.parameterTypes && method.parameterTypes.length > 0) ? `
                        <table class=\"text-xs mb-1\"><tr><td class=\"pr-2 text-muted-foreground\">Parameters</td><td class=\"font-mono\">${method.parameterTypes.join(', ')}</td></tr>
                        ${(method.parameterNames && method.parameterNames.length > 0) ? `<tr><td class=\"pr-2 text-muted-foreground\">Names</td><td class=\"font-mono\">${method.parameterNames.join(', ')}</td></tr>` : ''}
                        </table>
                      ` : ''}
                      ${method.description ? `<div class="prose max-w-none text-xs mt-1">${method.description.replace(/\n/g, '<br>')}</div>` : ''}
                    </div>
                  </div>
                  `;
                }).join('')}
              </div>
            </div>
          ` : ''}
        </div>
      `;

      setJavadoc(formattedJavadoc);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load JavaDocs');
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        onClick={() => {
          fetchJavadoc(className);
          setIsOpen(true);
        }}
        className="justify-start"
      >
        <ChevronDown className="mr-2 h-4 w-4" />
        JavaDocs
      </Button>

      <Dialog open={isOpen} onOpenChange={setIsOpen}>
        <DialogContent className="max-w-4xl w-[90vw] max-h-[90vh] flex flex-col">
          <DialogHeader className="pb-4">
            <DialogTitle className="text-lg">JavaDocs - {className}</DialogTitle>
          </DialogHeader>
          <div className="flex-1 overflow-y-auto pr-2 -mr-2">
            {loading ? (
              <div className="flex items-center justify-center py-8">
                <span className="text-muted-foreground">Loading JavaDocs...</span>
              </div>
            ) : error ? (
              <div className="text-center py-8">
                <p className="text-red-500">Error: {error}</p>
              </div>
            ) : javadoc ? (
              <div className="prose max-w-none pb-4">
                <div className="space-y-6" dangerouslySetInnerHTML={{ __html: javadoc }} />
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-muted-foreground">No JavaDocs available</p>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
