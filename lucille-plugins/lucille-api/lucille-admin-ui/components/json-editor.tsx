"use client"

export default function JsonEditor({ value, onChange }: { value: string, onChange: (val: string) => void }) {
  return (
    <div className="space-y-4">
      <div className="border rounded-md">
        <textarea
          className="w-full h-[500px] p-4 font-mono text-sm bg-muted/50 rounded-md focus:outline-none"
          value={value}
          onChange={(e) => onChange(e.target.value)}
        />
      </div>
      {/* Removed Format JSON and Validate buttons */}
    </div>
  )
}
