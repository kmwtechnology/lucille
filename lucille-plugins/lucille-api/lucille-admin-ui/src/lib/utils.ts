import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export type ApiInstant = string | number | null | undefined

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 Bytes"
  const k = 1024
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"]
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
}
/**
 * Normalize an API instant to epoch milliseconds, or NaN when absent/invalid.
 */
function toEpochMs(value: ApiInstant): number {
  if (value === null || value === undefined || value === "") return Number.NaN
  if (typeof value === "number") return value * 1000
  const asNumber = Number(value)
  if (!Number.isNaN(asNumber) && value.trim() !== "") return asNumber * 1000
  return new Date(value).getTime()
}

/** Format an API instant as a locale date-time string, or a dash when absent. */
export function formatInstant(value: ApiInstant): string {
  const ms = toEpochMs(value)
  if (Number.isNaN(ms)) return value ? String(value) : "—"
  return new Date(ms).toLocaleString()
}

export function formatDuration(
  start: ApiInstant,
  end: ApiInstant,
): string {
  if (start === null || start === undefined || start === "") return "—"
  const startMs = toEpochMs(start)
  const endMs = end === null || end === undefined || end === "" ? Date.now() : toEpochMs(end)
  if (Number.isNaN(startMs) || Number.isNaN(endMs)) return "—"

  const totalSeconds = Math.max(0, Math.round((endMs - startMs) / 1000))
  if (totalSeconds < 60) return `${totalSeconds}s`
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  if (minutes < 60) return `${minutes}m ${seconds}s`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ${minutes % 60}m`
}
