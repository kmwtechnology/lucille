import { NextRequest, NextResponse } from 'next/server';

export async function GET(_req: NextRequest) {
  try {
    const lucilleRes = await fetch(`${process.env.LUCILLE_API_URL || 'http://localhost:8080'}/v1/systemstats`, {
      method: 'GET',
      headers: process.env.LUCILLE_API_AUTH ? { 'authorization': process.env.LUCILLE_API_AUTH } : {},
    });
    const text = await lucilleRes.text();
    if (!lucilleRes.ok) {
      return NextResponse.json({
        ok: false,
        error: text || 'System stats fetch failed',
        status: lucilleRes.status,
        details: {},
      }, { status: lucilleRes.status });
    }
    const data = JSON.parse(text);
    return NextResponse.json(data, { status: 200 });
  } catch (error: unknown) {
    return NextResponse.json({
      ok: false,
      error: error instanceof Error ? error.message : 'Failed to connect to Lucille API',
      status: 502,
      details: {},
    }, { status: 502 });
  }
}
