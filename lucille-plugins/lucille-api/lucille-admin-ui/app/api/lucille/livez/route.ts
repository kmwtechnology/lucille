import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

export async function GET(_req: NextRequest) {
  // Proxy to /v1/livez
  // Next.js API routes use NextRequest/NextResponse in app dir
  // We wrap the proxy function for compatibility
  
  // Use a simple fetch for now
  try {
    const lucilleRes = await fetch(`${process.env.LUCILLE_API_URL || 'http://localhost:8080'}/v1/livez`, {
      method: 'GET',
      headers: process.env.LUCILLE_API_AUTH ? { 'authorization': process.env.LUCILLE_API_AUTH } : {},
    });
    const text = await lucilleRes.text();
    if (!lucilleRes.ok) {
      return NextResponse.json({
        ok: false,
        error: text || 'Liveness check failed',
        status: lucilleRes.status,
        details: {},
      }, { status: lucilleRes.status });
    }
    return NextResponse.json({ ok: true }, { status: 200 });
  } catch (error: unknown) {
    return NextResponse.json({
      ok: false,
      error: error instanceof Error ? error.message : 'Failed to connect to Lucille API',
      status: 502,
      details: {},
    }, { status: 502 });
  }
}
