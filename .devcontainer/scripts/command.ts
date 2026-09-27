import { spawnSync } from 'node:child_process';
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export const repositoryRoot = realpathSync(fileURLToPath(new URL('../..', import.meta.url)));

export interface CommandRunner {
  run(command: string, args: readonly string[], cwd?: string): void;
  capture(command: string, args: readonly string[], cwd?: string): string;
}

export class CommandFailure extends Error {
  readonly exitCode: number;

  constructor(command: string, exitCode: number | null, detail?: string) {
    super(`Command "${command}" failed${detail ? `: ${detail}` : ` with exit code ${String(exitCode)}`}.`);
    this.name = 'CommandFailure';
    this.exitCode = exitCode ?? 1;
  }
}

export class SpawnCommandRunner implements CommandRunner {
  run(command: string, args: readonly string[], cwd = repositoryRoot): void {
    this.invoke(command, args, cwd, false);
  }

  capture(command: string, args: readonly string[], cwd = repositoryRoot): string {
    return this.invoke(command, args, cwd, true);
  }

  private invoke(command: string, args: readonly string[], cwd: string, capture: boolean): string {
    const result = spawnSync(command, [...args], {
      cwd,
      encoding: 'utf8',
      shell: false,
      stdio: capture ? ['ignore', 'pipe', 'inherit'] : 'inherit',
    });
    if (result.error) {
      throw new CommandFailure(command, null, result.error.message);
    }
    if (result.status !== 0) {
      throw new CommandFailure(command, result.status);
    }
    return capture ? String(result.stdout) : '';
  }
}

export function runMain(name: string, action: () => void): void {
  try {
    action();
  } catch (error) {
    process.stderr.write(`${name}: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = error instanceof CommandFailure ? error.exitCode : 1;
  }
}
