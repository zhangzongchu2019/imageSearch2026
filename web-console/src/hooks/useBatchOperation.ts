import { useState, useCallback, useRef } from 'react';
import type { BatchProgress, BatchItemResult } from '../api/types';

export function useBatchOperation(sseUrl: string) {
  const [progress, setProgress] = useState<BatchProgress | null>(null);
  const [running, setRunning] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  const start = useCallback(
    (body: FormData | string) => {
      setRunning(true);
      setProgress({ completed: 0, total: 0, succeeded: 0, failed: 0, results: [] });

      // POST to initiate, then SSE for progress
      fetch(sseUrl, {
        method: 'POST',
        body,
        headers: typeof body === 'string' ? { 'Content-Type': 'application/json' } : undefined,
      }).then(async (res) => {
        if (!res.ok || !res.body) {
          setRunning(false);
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) {
            if (line.startsWith('data: ')) {
              try {
                const data = JSON.parse(line.slice(6));
                setProgress(data as BatchProgress);
              } catch {}
            }
          }
        }
        setRunning(false);
      });
    },
    [sseUrl],
  );

  const cancel = useCallback(() => {
    eventSourceRef.current?.close();
    setRunning(false);
  }, []);

  return { progress, running, start, cancel };
}
