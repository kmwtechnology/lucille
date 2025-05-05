import { NextRequest, NextResponse } from 'next/server';
import { getRun } from '../../../../../lib/api/lucille';

export async function GET(req: NextRequest, { params }: { params: { runId: string } }) {
  const { runId } = params;
  const { ok, status, data } = await getRun(runId);
  if (!ok) {
    return NextResponse.json(
      { ok: false, error: typeof data === 'string' ? data : data?.error || 'Unknown error from Lucille', details: data },
      { status }
    );
  }
  return NextResponse.json(data ?? { ok: true }, { status });
}
