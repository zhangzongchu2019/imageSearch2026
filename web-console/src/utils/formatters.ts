export function formatMs(ms: number): string {
  if (ms < 1) return '<1ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function formatTimestamp(ts: string | number): string {
  return new Date(ts).toLocaleString('zh-CN');
}

export function truncate(str: string, max: number): string {
  return str.length > max ? str.slice(0, max) + '...' : str;
}
