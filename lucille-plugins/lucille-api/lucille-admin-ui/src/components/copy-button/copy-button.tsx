import { useState } from "react"
import { Check, Copy } from "lucide-react"
import { Button } from "@/components/ui/button/button"
import styles from "./copy-button.module.css"

/** Icon button that copies the given value to the clipboard and briefly shows a check. */
export function CopyButton({ value, label }: { value: string; label: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
    }
  }

  return (
    <Button
      variant="ghost"
      size="sm"
      className={styles.copyButton}
      onClick={handleCopy}
      aria-label={label}
    >
      {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
    </Button>
  )
}
