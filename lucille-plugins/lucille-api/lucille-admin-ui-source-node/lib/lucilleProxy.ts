import type { NextApiRequest, NextApiResponse } from 'next';
import { log, logError } from './logger';

const LUCILLE_API_URL = process.env.LUCILLE_API_URL || 'http://localhost:8080';
const LUCILLE_API_AUTH = process.env.LUCILLE_API_AUTH;

export async function proxyLucille(
  req: NextApiRequest,
  res: NextApiResponse,
  {
    targetPath,
    methodOverride,
    logContext = {},
  }: {
    targetPath: string;
    methodOverride?: string;
    logContext?: Record<string, unknown>;
  }
) {
  const url = `${LUCILLE_API_URL}${targetPath}`;
  const method = methodOverride || req.method || 'GET';
  // Use a more specific type for headers to accommodate potential string arrays
  const headers: Record<string, string | string[]> = {
    ...req.headers as Record<string, string | string[]>,
    host: '', // Remove host header to avoid conflicts
  };
  if (LUCILLE_API_AUTH) {
    headers['authorization'] = LUCILLE_API_AUTH;
  }
  // Remove next.js-specific headers
  delete headers['connection'];
  delete headers['accept-encoding'];
  delete headers['content-length'];
  delete headers['content-type'];
  if (req.headers['content-type']) {
    headers['content-type'] = req.headers['content-type'] as string;
  }

  // Define body with a more specific type for fetch compatibility
  let body: string | undefined = undefined;
  if (method !== 'GET' && method !== 'HEAD') {
    body = req.body && typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
  }

  try {
    log('info', `Proxying to Lucille: ${method} ${url}`, logContext);
    
    // Convert headers to HeadersInit type for fetch compatibility
    const fetchHeaders = new Headers();
    for (const [key, value] of Object.entries(headers)) {
      if (Array.isArray(value)) {
        // For arrays like set-cookie, add each value separately
        value.forEach(v => fetchHeaders.append(key, v));
      } else if (value !== undefined) {
        fetchHeaders.append(key, value);
      }
    }
    
    const lucilleRes = await fetch(url, {
      method,
      headers: fetchHeaders,
      body,
    });
    const text = await lucilleRes.text();
    let data: unknown;
    try {
      data = text ? JSON.parse(text) : undefined;
    } catch {
      data = text;
    }
    if (!lucilleRes.ok) {
      log('warn', `Lucille error response: ${lucilleRes.status}`, { ...logContext, status: lucilleRes.status, data });
      return res.status(lucilleRes.status).json({
        ok: false,
        error: typeof data === 'string' ? data : (data as Record<string, unknown>)?.error?.toString() || 'Unknown error from Lucille',
        status: lucilleRes.status,
        details: data,
      });
    }
    return res.status(lucilleRes.status).json(data ?? { ok: true });
  } catch (error) {
    logError('Proxy error to Lucille', error, logContext);
    return res.status(502).json({
      ok: false,
      error: 'Failed to connect to Lucille API',
      status: 502,
      details: {},
    });
  }
}
