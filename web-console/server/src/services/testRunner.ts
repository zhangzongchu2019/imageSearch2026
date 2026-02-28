import { spawn, type ChildProcess } from 'child_process';
import { v4 as uuid } from 'uuid';
import { config } from '../config.js';

interface TestRunState {
  process: ChildProcess;
  output: string;
  status: 'running' | 'completed' | 'failed';
  passed: number;
  failed: number;
  errors: number;
}

const runs = new Map<string, TestRunState>();

const serviceTargetMap: Record<string, Record<string, string>> = {
  'search-service': { unit: 'test-search', integration: 'test-search-integ' },
  'write-service': { unit: 'test-write', integration: 'test-write-integ' },
  'cron-scheduler': { unit: 'test-lifecycle', integration: 'test-lifecycle-integ' },
  'flink-pipeline': { unit: 'test-flink', integration: 'test-flink-integ' },
  'bitmap-filter-service': { unit: 'test-bitmap', integration: 'test-bitmap-integ' },
  all: { unit: 'test-all', integration: 'test-all-integ' },
};

export function startTestRun(service: string, type: 'unit' | 'integration'): string {
  const runId = uuid();
  const target = serviceTargetMap[service]?.[type];
  if (!target) throw new Error(`Unknown service/type: ${service}/${type}`);

  const proc = spawn('make', [target], {
    cwd: config.projectRoot,
    shell: true,
  });

  const state: TestRunState = {
    process: proc,
    output: '',
    status: 'running',
    passed: 0,
    failed: 0,
    errors: 0,
  };

  proc.stdout?.on('data', (chunk: Buffer) => {
    state.output += chunk.toString();
  });

  proc.stderr?.on('data', (chunk: Buffer) => {
    state.output += chunk.toString();
  });

  proc.on('close', (code) => {
    state.status = code === 0 ? 'completed' : 'failed';
    // Parse test counts from output
    const passMatch = state.output.match(/(\d+) passed/);
    const failMatch = state.output.match(/(\d+) failed/);
    const errMatch = state.output.match(/(\d+) error/);
    if (passMatch) state.passed = parseInt(passMatch[1]);
    if (failMatch) state.failed = parseInt(failMatch[1]);
    if (errMatch) state.errors = parseInt(errMatch[1]);
  });

  runs.set(runId, state);

  // Clean up after 30 minutes
  setTimeout(() => runs.delete(runId), 30 * 60_000);

  return runId;
}

export function getTestRun(runId: string): TestRunState | undefined {
  return runs.get(runId);
}
