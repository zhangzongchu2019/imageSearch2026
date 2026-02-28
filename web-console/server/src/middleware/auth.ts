import type { Request, Response, NextFunction } from 'express';

export function injectApiKey(req: Request, _res: Response, next: NextFunction) {
  // API Key is managed server-side; inject from env or session
  const apiKey = process.env.IMGSRCH_API_KEY || req.headers['x-api-key'];
  if (apiKey) {
    req.headers['x-api-key'] = apiKey as string;
  }
  next();
}
