/**
 * docker-control.ts
 *
 * Utilities for controlling Docker containers during reliability tests.
 * Uses the Docker Engine API (via Unix socket or TCP) to start, stop, pause,
 * and resume the test-stack containers without leaving the Jest process.
 *
 * Containers are identified by their names from docker-compose.test.yml:
 *   showcase-test-rabbitmq
 *   showcase-test-postgres
 *   showcase-test-messaging
 *   showcase-test-gateway
 *
 * All operations use `docker` CLI via child_process.execSync rather than
 * the Docker HTTP API — simpler, no extra deps, same semantics in CI and
 * local dev.
 */

import { execSync, spawnSync } from 'child_process';

export const CONTAINERS = {
  RABBITMQ: 'showcase-test-rabbitmq',
  POSTGRES: 'showcase-test-postgres',
  MESSAGING: 'showcase-test-messaging',
  GATEWAY: 'showcase-test-gateway',
} as const;

export type ContainerName = (typeof CONTAINERS)[keyof typeof CONTAINERS];

/**
 * Stop a container (SIGTERM then SIGKILL after timeout).
 * Equivalent to `docker stop <name>`.
 */
export function containerStop(name: ContainerName, timeoutSec = 5): void {
  execSync(`docker stop -t ${timeoutSec} ${name}`, { stdio: 'pipe' });
}

/**
 * Start a previously stopped container.
 * Equivalent to `docker start <name>`.
 */
export function containerStart(name: ContainerName): void {
  execSync(`docker start ${name}`, { stdio: 'pipe' });
}

/**
 * Pause a container (SIGSTOP — freezes all processes, keeps state in memory).
 * Simulates a fully unresponsive but not dead service.
 */
export function containerPause(name: ContainerName): void {
  execSync(`docker pause ${name}`, { stdio: 'pipe' });
}

/**
 * Resume a paused container.
 */
export function containerUnpause(name: ContainerName): void {
  execSync(`docker unpause ${name}`, { stdio: 'pipe' });
}

/**
 * Kill a container immediately with SIGKILL — no graceful shutdown.
 * Simulates an abrupt crash (power loss, OOM kill, etc.).
 */
export function containerKill(name: ContainerName): void {
  execSync(`docker kill ${name}`, { stdio: 'pipe' });
}

/**
 * Send SIGTERM to a container's main process — simulates a graceful
 * shutdown request (Kubernetes pod termination, `docker stop`, etc.).
 */
export function containerSigterm(name: ContainerName): void {
  execSync(`docker kill --signal SIGTERM ${name}`, { stdio: 'pipe' });
}

/**
 * Check if a container is currently running.
 */
export function isContainerRunning(name: ContainerName): boolean {
  const result = spawnSync(
    'docker',
    ['inspect', '--format', '{{.State.Running}}', name],
    { encoding: 'utf8' },
  );
  return result.stdout.trim() === 'true';
}

/**
 * Wait until a container is running (after a start).
 */
export async function waitForContainerRunning(
  name: ContainerName,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (isContainerRunning(name)) return;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`Container ${name} not running after ${timeoutMs}ms`);
}

/**
 * Wait until a container health status is "healthy".
 */
export async function waitForContainerHealthy(
  name: ContainerName,
  timeoutMs = 120_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = spawnSync(
      'docker',
      ['inspect', '--format', '{{.State.Health.Status}}', name],
      { encoding: 'utf8' },
    );
    const status = result.stdout.trim();
    if (status === 'healthy') return;
    await new Promise((r) => setTimeout(r, 1_000));
  }
  throw new Error(`Container ${name} not healthy after ${timeoutMs}ms`);
}

/**
 * Execute a command inside a running container and return stdout.
 * Used for injecting failures at the OS level (e.g. killing specific PIDs).
 */
export function containerExec(name: ContainerName, cmd: string): string {
  const result = spawnSync('docker', ['exec', name, 'sh', '-c', cmd], {
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error(
      `containerExec(${name}, "${cmd}") failed: ${result.stderr ?? result.stdout}`,
    );
  }
  return result.stdout;
}

/**
 * Get container logs (tail N lines).
 */
export function containerLogs(name: ContainerName, tail = 50): string {
  const result = spawnSync('docker', ['logs', '--tail', String(tail), name], {
    encoding: 'utf8',
  });
  return result.stdout + result.stderr;
}

/**
 * Blocking helper: stop a container, wait for it to be gone, then start it
 * and wait for it to be healthy again. Returns the total downtime in ms.
 */
export async function restartContainer(
  name: ContainerName,
  opts: { stopTimeoutSec?: number; healthyTimeoutMs?: number } = {},
): Promise<number> {
  const { stopTimeoutSec = 5, healthyTimeoutMs = 120_000 } = opts;

  const stopTime = Date.now();
  containerStop(name, stopTimeoutSec);
  containerStart(name);

  await waitForContainerRunning(name);
  // Only wait for health if the container has a healthcheck
  try {
    await waitForContainerHealthy(name, healthyTimeoutMs);
  } catch {
    // Some containers don't have healthchecks — ignore
  }

  return Date.now() - stopTime;
}
