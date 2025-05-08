import { NextRequest, NextResponse } from 'next/server';
import { getRun } from '../../../../../lib/api/lucille';

export const dynamic = "force-static";

export function generateStaticParams() {
  // If you have specific run IDs that should be pre-rendered at build time,
  // you can fetch them here and return them as an array of objects.
  // For example: return [{ runId: 'run1' }, { runId: 'run2' }];
  // For now, returning an empty array means no specific instances of this dynamic API route
  // will be pre-rendered as separate JSON files during the build.
  return [];
}

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
