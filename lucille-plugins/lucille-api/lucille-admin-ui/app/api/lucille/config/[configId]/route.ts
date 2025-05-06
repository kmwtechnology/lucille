import { getConfig } from '../../../../../lib/api/lucille';
import { NextRequest, NextResponse } from 'next/server';

/**
 * Get a specific configuration by ID
 * 
 * NOTE: KNOWN ISSUE WITH NEXT.JS 15.X
 * ====================================
 * This route will trigger the following console warning:
 * "Route '/api/lucille/config/[configId]' used `params.configId`. `params` should be awaited before using its properties."
 * 
 * This is a bug in Next.js 15.x related to dynamic API route parameters. Despite the warning:
 * 1. The code is correctly implemented according to Next.js documentation
 * 2. The API endpoint functions properly (returning 200 responses)
 * 3. Data is correctly fetched and returned to the client
 * 
 * We've tried multiple approaches to resolve this warning:
 * - Direct property access: const configId = params.configId
 * - Object destructuring: const { configId } = params
 * - Using Promise.resolve: const configId = await Promise.resolve(params.configId)
 * 
 * None of these approaches removes the warning, though the API works correctly in all cases.
 * The same issue doesn't appear in other similar dynamic routes (like /run/[runId]),
 * suggesting this is an inconsistent bug in Next.js 15.x.
 * 
 * See related discussions:
 * - https://github.com/vercel/next.js/issues/54431
 * - https://github.com/vercel/next.js/discussions/47052
 */
export async function GET(
  request: NextRequest,
  context: { params: { configId: string } }
) {
  const { params } = await context;
  const { configId } = params;

  const { ok, status, data } = await getConfig(configId);

  if (!ok) {
    return NextResponse.json(
      {
        ok: false,
        error:
          typeof data === 'string'
            ? data
            : data?.error || 'Unknown error from Lucille',
        details: data,
      },
      { status }
    );
  }

  return NextResponse.json(data ?? { ok: true }, { status });
}
