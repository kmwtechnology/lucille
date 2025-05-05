import { NextRequest, NextResponse } from 'next/server';
import { getRuns, createRun } from '../../../../lib/api/lucille';

export async function GET(_req: NextRequest) {
  const { ok, status, data } = await getRuns();
  if (!ok) {
    return NextResponse.json(
      { ok: false, error: typeof data === 'string' ? data : data?.error || 'Unknown error from Lucille', details: data },
      { status }
    );
  }
  return NextResponse.json(data ?? { ok: true }, { status });
}

export async function POST(req: NextRequest) {
  const { configId } = await req.json();
  const { ok, status, data } = await createRun(configId);
  if (!ok) {
    return NextResponse.json(
      { ok: false, error: typeof data === 'string' ? data : data?.error || 'Unknown error from Lucille', details: data },
      { status }
    );
  }
  return NextResponse.json(data ?? { ok: true }, { status });
}
