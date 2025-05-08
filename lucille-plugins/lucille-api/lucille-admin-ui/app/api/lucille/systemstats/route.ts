import { NextRequest, NextResponse } from 'next/server';

export const dynamic = "force-static";

export  function generateStaticParams() {
  // If you have specific run IDs that should be pre-rendered at build time,
  // you can fetch them here and return them as an array of objects.
  // For example: return [{ runId: 'run1' }, { runId: 'run2' }];
  // For now, returning an empty array means no specific instances of this dynamic API route
  // will be pre-rendered as separate JSON files during the build.
  return [];
}


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
