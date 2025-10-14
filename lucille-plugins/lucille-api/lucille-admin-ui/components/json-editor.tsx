"use client"

export default function JsonEditor({ value, onChange }: { value: string, onChange: (val: string) => void }) {
  return (
    <textarea
      className="w-full min-h-[200px] max-h-[70vh] p-4 font-mono text-sm bg-gray-50 focus:outline-none border-0 rounded-none"
      style={{ boxSizing: 'border-box', lineHeight: '1.5', resize: 'vertical' }}
      value={value}
      onChange={e => onChange(e.target.value)}
      spellCheck={false}
      rows={Math.max(10, value.split('\n').length)}
    />
  )
}
