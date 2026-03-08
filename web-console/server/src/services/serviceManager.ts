import { spawn, type ChildProcess } from 'child_process';
import { v4 as uuid } from 'uuid';
import { config } from '../config.js';

const SERVICE_WHITELIST = new Set([
  'search-service',
  'write-service',
  'inference-service',
  'cron-scheduler',
  'bitmap-filter-service',
  'flink-jobmanager',
  'flink-taskmanager',
  'postgres',
  'redis',
  'kafka',
  'milvus-standalone',
  'etcd',
  'minio',
  'clickhouse',
  'prometheus',
  'grafana',
]);

export interface ServiceInfo {
  name: string;
  state: string;   // running, exited, etc.
  status: string;   // full status text
  ports: string;
}

const logStreams = new Map<string, ChildProcess>();

export function isAllowedService(name: string): boolean {
  return SERVICE_WHITELIST.has(name);
}

export async function getServiceStatus(): Promise<ServiceInfo[]> {
  return new Promise((resolve, reject) => {
    // Try docker compose v2 JSON format first, fallback to v1 text parsing
    // 优先尝试 docker compose v2, 回退到 docker-compose v1
    const useV2 = process.env.DOCKER_COMPOSE_V2 !== 'false';
    const cmd = useV2 ? 'docker' : 'docker-compose';
    const cmdArgs = useV2 ? ['compose', 'ps', '--format', 'json', '-a'] : ['ps'];
    const proc = spawn(cmd, cmdArgs, {
      cwd: config.projectRoot,
      shell: true,
    });

    let output = '';
    proc.stdout?.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });

    let stderr = '';
    proc.stderr?.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`docker compose ps failed: ${stderr}`));
        return;
      }

      const services: ServiceInfo[] = [];
      const found = new Set<string>();

      // docker compose v2 --format json outputs one JSON object per line (NDJSON)
      const lines = output.trim().split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const container = JSON.parse(line);
          const serviceName = container.Service || container.service || '';
          if (!serviceName || !SERVICE_WHITELIST.has(serviceName)) continue;
          if (found.has(serviceName)) continue;
          found.add(serviceName);

          // Normalize state from v2 JSON (State: "running", "exited", "restarting", etc.)
          const rawState = (container.State || container.state || '').toLowerCase();
          let state = 'unknown';
          if (rawState === 'running') state = 'running';
          else if (rawState === 'exited') state = 'exited';
          else if (rawState === 'restarting') state = 'restarting';
          else if (rawState === 'created') state = 'created';
          else state = rawState || 'unknown';

          const statusText = container.Status || container.status || rawState;
          const portsText = container.Ports || container.ports || '';

          services.push({
            name: serviceName,
            state,
            status: statusText,
            ports: typeof portsText === 'string' ? portsText : JSON.stringify(portsText),
          });
        } catch {
          // If JSON parse fails, try v1 text parsing as fallback
          const containerName = line.trim().split(/\s{2,}/)[0];
          if (!containerName) continue;

          // v1: <project>_<service>_<number>, v2: <project>-<service>-<number>
          const match = containerName.match(/^[^_-]+[_-](.+?)[_-]\d+$/);
          if (!match) continue;

          const serviceName = match[1];
          if (!SERVICE_WHITELIST.has(serviceName) || found.has(serviceName)) continue;
          found.add(serviceName);

          const cols = line.trim().split(/\s{2,}/);
          const stateCol = cols[2] || '';
          const portsCol = cols[3] || '';

          let state = 'unknown';
          if (/^Up/i.test(stateCol)) state = 'running';
          else if (/^Exit/i.test(stateCol)) state = 'exited';
          else if (/^Restarting/i.test(stateCol)) state = 'restarting';

          services.push({ name: serviceName, state, status: stateCol, ports: portsCol });
        }
      }

      // Add missing whitelisted services as "not found"
      for (const svc of SERVICE_WHITELIST) {
        if (!found.has(svc)) {
          services.push({ name: svc, state: 'not_found', status: '', ports: '' });
        }
      }

      services.sort((a, b) => a.name.localeCompare(b.name));
      resolve(services);
    });
  });
}

export function controlService(
  name: string,
  action: 'start' | 'stop' | 'restart',
): Promise<string> {
  return new Promise((resolve, reject) => {
    if (!isAllowedService(name)) {
      reject(new Error(`Service not allowed: ${name}`));
      return;
    }

    const useV2 = process.env.DOCKER_COMPOSE_V2 !== 'false';
    const cmd = useV2 ? 'docker' : 'docker-compose';
    const cmdArgs = useV2 ? ['compose', action, name] : [action, name];
    const proc = spawn(cmd, cmdArgs, {
      cwd: config.projectRoot,
      shell: true,
    });

    let output = '';
    proc.stdout?.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });
    proc.stderr?.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`docker-compose ${action} ${name} failed: ${output}`));
      } else {
        resolve(output || `${action} ${name} succeeded`);
      }
    });
  });
}

export function streamLogs(
  name: string,
  tail: number = 200,
): { streamId: string; process: ChildProcess } {
  if (!isAllowedService(name)) {
    throw new Error(`Service not allowed: ${name}`);
  }

  const streamId = uuid();
  const useV2 = process.env.DOCKER_COMPOSE_V2 !== 'false';
  const cmd = useV2 ? 'docker' : 'docker-compose';
  const cmdArgs = useV2 ? ['compose', 'logs', '-f', '--tail', String(tail), name] : ['logs', '-f', '--tail', String(tail), name];
  const proc = spawn(cmd, cmdArgs, {
    cwd: config.projectRoot,
    shell: true,
  });

  logStreams.set(streamId, proc);

  proc.on('close', () => {
    logStreams.delete(streamId);
  });

  return { streamId, process: proc };
}

export function stopLogStream(streamId: string): boolean {
  const proc = logStreams.get(streamId);
  if (proc) {
    proc.kill();
    logStreams.delete(streamId);
    return true;
  }
  return false;
}
