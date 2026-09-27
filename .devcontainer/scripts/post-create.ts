#!/usr/bin/env node

import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { repositoryRoot, runMain, SpawnCommandRunner } from './command.ts';

const commands = new SpawnCommandRunner();
const toolBin = '/home/vscode/.local/bin';

function requireVersion(name: string, command: string, args: readonly string[], expected: RegExp): void {
  const actual = commands.capture(command, args).trim();
  if (!expected.test(actual)) {
    throw new Error(`${name}: unexpected version "${actual}".`);
  }
  process.stdout.write(`${name}: ${actual}\n`);
}

runMain('post-create', () => {
  commands.run('npm', [
    'install',
    '--ignore-scripts',
    '--no-audit',
    '--no-fund',
    '--package-lock=false',
    '--registry=https://registry.npmjs.org/',
  ]);
  commands.run('uv', ['tool', 'install', '--python', 'python3.12', 'hatch==1.18.1']);
  commands.run('uv', ['tool', 'install', '--python', 'python3.12', 'ms-fabric-cli @ git+https://github.com/microsoft/fabric-cli.git@0183fbf1809826040ed4805e6163cb51c1613cb9']);

  rmSync(join(repositoryRoot, '.venv'), { force: true, recursive: true });
  commands.run(join(toolBin, 'hatch'), ['env', 'create']);
  const venvBin = join(repositoryRoot, '.venv', 'bin');

  requireVersion('Python', 'python', ['--version'], /^Python 3\.12\./u);
  requireVersion('Node.js', 'node', ['--version'], /^v24\./u);
  requireVersion('Azure CLI', 'az', ['version', '--query', '"azure-cli"', '--output', 'tsv'], /^\d+\./u);
  requireVersion('uv', 'uv', ['--version'], /^uv \d+\./u);
  commands.run(join(toolBin, 'hatch'), ['--version']);
  commands.run(join(venvBin, 'pyinstaller'), ['--version']);
  commands.run(join(toolBin, 'fab'), ['--version']);
});
