const MERCHANT_PREFIX = 'T20260101';
const MERCHANT_MIN = 10001;
const MERCHANT_MAX = 29999;

export function randomMerchantId(): string {
  const num = MERCHANT_MIN + Math.floor(Math.random() * (MERCHANT_MAX - MERCHANT_MIN + 1));
  return MERCHANT_PREFIX + num;
}

export function randomProductId(): string {
  const hex = Array.from({ length: 8 }, () =>
    Math.floor(Math.random() * 16).toString(16),
  ).join('');
  return `TEST-PROD-${hex}`;
}

export function generateMerchantScope(count: number): string[] {
  const total = MERCHANT_MAX - MERCHANT_MIN + 1;
  const step = total / count;
  const result: string[] = [];
  for (let i = 0; i < count; i++) {
    const num = MERCHANT_MIN + Math.floor(i * step);
    result.push(MERCHANT_PREFIX + num);
  }
  return result;
}
