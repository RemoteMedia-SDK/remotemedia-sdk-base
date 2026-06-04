#!/usr/bin/env node
// Wrapper for the typed-RPC E2E suite. Sets the env vars the suite
// needs BEFORE spawning jest as a child process — Node's
// `process.env = ...` assignments inside a jest worker don't reach the
// libc `environ` table that the Rust-side plugin's `std::env::var`
// reads, so the var has to be in the inherited env at worker spawn.
//
// Env vars set:
//   - RPC_E2E=1                       — gates the suite (skipped on plain `npm test`)
//   - REMOTEMEDIA_PYTHON_SRC          — points at `<repo>/clients/python` so the
//                                       managed venv editable-installs the
//                                       `remotemedia` package and the runner
//                                       subprocess can `import remotemedia.*`.
//                                       Skipped if already exported.
//   - REMOTEMEDIA_PLUGIN_STDERR_FILE  — redirects the Python runner's stderr to
//                                       a file. Required under jest because the
//                                       jest worker's inherited stderr is a
//                                       captured stream that SIGPIPE-kills the
//                                       runner mid-startup with exit status 1,
//                                       which the subprocess-death watchdog then
//                                       surfaces as a false-positive crash.
//                                       Skipped if already exported.
//
// Forwards any extra args to jest so `npm run test:e2e-rpc -- --verbose`
// works.

const { spawn } = require('node:child_process');
const path = require('node:path');

process.env.RPC_E2E = '1';

if (!process.env.REMOTEMEDIA_PYTHON_SRC) {
  // This script sits at `crates/transports/ffi/nodejs/scripts/`.
  // The Python client package is at `<repo>/clients/python`.
  process.env.REMOTEMEDIA_PYTHON_SRC = path.resolve(
    __dirname,
    '..',
    '..',
    '..',
    '..',
    '..',
    'clients',
    'python',
  );
}

// Redirect the runner subprocess's stderr to a file. Jest captures the
// worker's stderr via a wrapped stream that doesn't behave like a normal
// inherited pipe — writes from the spawned Python process eventually
// hit a closed-fd state and SIGPIPE-kill Python with exit status 1
// (which the new fail-fast subprocess-death check then surfaces as
// "runner process exited before publishing READY"). Pointing the
// runner at a file sidesteps the jest stderr-capture entirely so
// Python lives long enough to publish READY. Skipped if the caller
// already exported a path (e.g. for explicit debugging).
if (!process.env.REMOTEMEDIA_PLUGIN_STDERR_FILE) {
  process.env.REMOTEMEDIA_PLUGIN_STDERR_FILE = path.join(
    require('node:os').tmpdir(),
    `remotemedia-rpc-e2e-${process.pid}.stderr.log`,
  );
}

const packageDir = path.resolve(__dirname, '..');
const jestBin = path.join(packageDir, 'node_modules', '.bin', 'jest');

const child = spawn(
  jestBin,
  ['rpc-proxy-e2e.test.ts', ...process.argv.slice(2)],
  {
    stdio: 'inherit',
    env: process.env,
    cwd: packageDir,
  },
);

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
