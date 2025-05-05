import { NextRequest, NextResponse } from 'next/server';

export async function GET(_req: NextRequest) {
  try {
    const lucilleRes = await fetch(`${process.env.LUCILLE_API_URL || 'http://localhost:8080'}/v1/config`, {
      method: 'GET',
      headers: process.env.LUCILLE_API_AUTH ? { 'authorization': process.env.LUCILLE_API_AUTH } : {},
    });
    const text = await lucilleRes.text();
    let data;
    try { data = text ? JSON.parse(text) : undefined; } catch { data = text; }
    if (!lucilleRes.ok) {
      return NextResponse.json({ ok: false, error: typeof data === 'string' ? data : data?.error || 'Unknown error from Lucille', status: lucilleRes.status, details: data }, { status: lucilleRes.status });
    }
    return NextResponse.json(data ?? { ok: true }, { status: lucilleRes.status });
  } catch (error: unknown) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : 'Failed to connect to Lucille API', status: 502, details: {} }, { status: 502 });
  }
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.text();
    const lucilleRes = await fetch(`${process.env.LUCILLE_API_URL || 'http://localhost:8080'}/v1/config`, {
      method: 'POST',
      headers: {
        ...(process.env.LUCILLE_API_AUTH ? { 'authorization': process.env.LUCILLE_API_AUTH } : {}),
        'content-type': req.headers.get('content-type') || 'application/json',
      },
      body,
    });
    const text = await lucilleRes.text();
    let data;
    try { data = text ? JSON.parse(text) : undefined; } catch { data = text; }
    if (!lucilleRes.ok) {
      return NextResponse.json({ ok: false, error: typeof data === 'string' ? data : data?.error || 'Unknown error from Lucille', status: lucilleRes.status, details: data }, { status: lucilleRes.status });
    }
    return NextResponse.json(data ?? { ok: true }, { status: lucilleRes.status });
  } catch (error: unknown) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : 'Failed to connect to Lucille API', status: 502, details: {} }, { status: 502 });
  }
}
