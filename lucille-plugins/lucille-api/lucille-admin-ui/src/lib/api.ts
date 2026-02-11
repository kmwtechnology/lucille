const API_BASE = import.meta.env.VITE_API_BASE ?? ""

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

async function healthCheck(path: string): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}${path}`)
    return res.ok
  } catch {
    return false
  }
}

export const api = { get, healthCheck }
