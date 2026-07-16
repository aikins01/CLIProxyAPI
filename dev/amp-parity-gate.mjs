import { spawnSync } from 'node:child_process';

const root = new URL('..', import.meta.url).pathname.replace(/\/$/, '');

const commands = [
  {
    name: 'worktree status',
    cmd: 'git',
    args: ['status', '--short', '--branch']
  },
  {
    name: 'local Amp version',
    cmd: 'zsh',
    args: ['-lc', "strings /Users/aikins01/.amp/bin/amp | rg -o '0\\.0\\.[0-9]+-g[0-9a-f]+' | sort -u | tail -1"]
  },
  {
    name: 'npm Amp latest',
    cmd: 'npm',
    args: ['view', '@ampcode/cli', 'version', 'dist-tags', '--json']
  },
  {
    name: 'installed proxy version',
    cmd: '/opt/homebrew/bin/cliproxyapi',
    args: ['-version'],
    allowFailureText: 'CLIProxyAPI Version:'
  },
  {
    name: 'proxy health',
    cmd: 'curl',
    args: ['-fsS', 'http://127.0.0.1:8317/healthz']
  },
  {
    name: 'binary audit',
    cmd: 'go',
    args: ['run', './cmd/amp_binary_audit', '-strict']
  },
  {
    name: 'web contract audit',
    cmd: 'go',
    args: ['run', './cmd/amp_web_audit', '-strict']
  },
  {
    name: 'prompt family audit',
    cmd: 'bun',
    args: ['dev/amp-prompt-family-audit.mjs']
  },
  {
    name: 'post-runtime drift scan',
    cmd: 'go',
    args: ['run', './cmd/amp_runtime_drift_scan', '-since-homebrew-runtime', '-summary', '-require-capture-summary']
  },
  {
    name: 'remote UI smoke',
    cmd: 'bun',
    args: ['run', '--cwd', 'dev/neo-remote-ui', 'smoke']
  }
];

for (const command of commands) {
  console.log(`\n==> ${command.name}`);
  console.log(`$ ${[command.cmd, ...command.args].join(' ')}`);
  const result = spawnSync(command.cmd, command.args, {
    cwd: root,
    stdio: command.allowFailureText ? 'pipe' : 'inherit',
    encoding: command.allowFailureText ? 'utf8' : undefined,
    env: process.env
  });
  if (command.allowFailureText) {
    const output = `${result.stdout ?? ''}${result.stderr ?? ''}`;
    if (result.status !== 0 && output.includes(command.allowFailureText)) {
      const line = output
        .split('\n')
        .find((item) => item.includes(command.allowFailureText));
      if (line) {
        console.log(line);
      }
    } else {
      if (result.stdout) {
        process.stdout.write(result.stdout);
      }
      if (result.stderr) {
        process.stderr.write(result.stderr);
      }
    }
  }
  if (result.error) {
    console.error(`${command.name} failed to start: ${result.error.message}`);
    process.exit(1);
  }
  if (
    result.status !== 0 &&
    command.allowFailureText &&
    `${result.stdout ?? ''}${result.stderr ?? ''}`.includes(command.allowFailureText)
  ) {
    continue;
  }
  if (result.status !== 0) {
    console.error(`${command.name} failed with exit code ${result.status}`);
    process.exit(result.status ?? 1);
  }
}

console.log('\nAmp parity gate passed');
