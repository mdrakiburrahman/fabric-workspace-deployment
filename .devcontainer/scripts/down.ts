#!/usr/bin/env node

import { repositoryRoot, runMain, SpawnCommandRunner } from './command.ts';

const commands = new SpawnCommandRunner();
const scopeLabel = 'com.github.mdrakiburrahman.fabric-workspace-deployment.devcontainer=true';

runMain('devcontainer-down', () => {
  const containerIds = commands
    .capture('docker', ['ps', '--all', '--quiet', '--filter', `label=${scopeLabel}`, '--filter', `label=devcontainer.local_folder=${repositoryRoot}`])
    .split(/\r?\n/u)
    .map((value) => value.trim())
    .filter(Boolean);

  if (containerIds.length === 0) {
    process.stdout.write(`No devcontainer found for ${repositoryRoot}.\n`);
    return;
  }
  commands.run('docker', ['rm', '--force', ...containerIds]);
});
