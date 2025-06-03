const levels = ['error', 'warn', 'info', 'debug'] as const;
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

type LogLevel = typeof levels[number];

function shouldLog(level: LogLevel) {
  return levels.indexOf(level) <= levels.indexOf(LOG_LEVEL as LogLevel);
}

export function log(level: LogLevel, message: string, meta?: Record<string, unknown>) {
  if (shouldLog(level)) {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...(meta || {}),
    };
    // Print as JSON for structured logging
    console.log(JSON.stringify(entry));
  }
}

export function logError(message: string, error: unknown, meta?: Record<string, unknown>) {
  log('error', message, {
    ...meta,
    error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
  });
}
