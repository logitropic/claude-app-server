#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const require = createRequire(import.meta.url);

const PLATFORM_PACKAGE_BY_TARGET = {
  "x86_64-unknown-linux-musl": "@logitropic/claude-app-server-linux-x64",
  "aarch64-unknown-linux-musl": "@logitropic/claude-app-server-linux-arm64",
  "x86_64-apple-darwin": "@logitropic/claude-app-server-darwin-x64",
  "aarch64-apple-darwin": "@logitropic/claude-app-server-darwin-arm64",
  "x86_64-pc-windows-msvc": "@logitropic/claude-app-server-win32-x64",
  "aarch64-pc-windows-msvc": "@logitropic/claude-app-server-win32-arm64",
};

function targetTripleForCurrentPlatform() {
  const { platform, arch } = process;

  if ((platform === "linux" || platform === "android") && arch === "x64") {
    return "x86_64-unknown-linux-musl";
  }
  if ((platform === "linux" || platform === "android") && arch === "arm64") {
    return "aarch64-unknown-linux-musl";
  }
  if (platform === "darwin" && arch === "x64") {
    return "x86_64-apple-darwin";
  }
  if (platform === "darwin" && arch === "arm64") {
    return "aarch64-apple-darwin";
  }
  if (platform === "win32" && arch === "x64") {
    return "x86_64-pc-windows-msvc";
  }
  if (platform === "win32" && arch === "arm64") {
    return "aarch64-pc-windows-msvc";
  }

  return null;
}

function detectPackageManager() {
  const userAgent = process.env.npm_config_user_agent || "";
  if (/\bbun\//.test(userAgent)) {
    return "bun";
  }

  const execPath = process.env.npm_execpath || "";
  if (execPath.includes("bun")) {
    return "bun";
  }

  if (
    __dirname.includes(".bun/install/global") ||
    __dirname.includes(".bun\\install\\global")
  ) {
    return "bun";
  }

  return userAgent ? "npm" : null;
}

function reinstallCommand() {
  return detectPackageManager() === "bun"
    ? "bun install -g @logitropic/claude-app-server@latest"
    : "npm install -g @logitropic/claude-app-server@latest";
}

const targetTriple = targetTripleForCurrentPlatform();
if (!targetTriple) {
  throw new Error(`Unsupported platform: ${process.platform} (${process.arch})`);
}

const platformPackage = PLATFORM_PACKAGE_BY_TARGET[targetTriple];
if (!platformPackage) {
  throw new Error(`Unsupported target triple: ${targetTriple}`);
}

const binaryName =
  process.platform === "win32" ? "claude-app-server.exe" : "claude-app-server";
const localVendorRoot = path.join(__dirname, "..", "vendor");
const localBinaryPath = path.join(
  localVendorRoot,
  targetTriple,
  "claude-app-server",
  binaryName,
);

let vendorRoot;
try {
  const packageJsonPath = require.resolve(`${platformPackage}/package.json`);
  vendorRoot = path.join(path.dirname(packageJsonPath), "vendor");
} catch {
  if (existsSync(localBinaryPath)) {
    vendorRoot = localVendorRoot;
  } else {
    throw new Error(
      `Missing optional dependency ${platformPackage}. Reinstall claude-app-server: ${reinstallCommand()}`,
    );
  }
}

const binaryPath = path.join(
  vendorRoot,
  targetTriple,
  "claude-app-server",
  binaryName,
);
const child = spawn(binaryPath, process.argv.slice(2), {
  stdio: "inherit",
  env: { ...process.env, CLAUDE_APP_SERVER_MANAGED_BY_NPM: "1" },
});

child.on("error", (err) => {
  console.error(err);
  process.exit(1);
});

const forwardSignal = (signal) => {
  if (child.killed) {
    return;
  }
  try {
    child.kill(signal);
  } catch {
    /* ignore */
  }
};

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => forwardSignal(signal));
}

const childResult = await new Promise((resolve) => {
  child.on("exit", (code, signal) => {
    if (signal) {
      resolve({ type: "signal", signal });
    } else {
      resolve({ type: "code", exitCode: code ?? 1 });
    }
  });
});

if (childResult.type === "signal") {
  process.kill(process.pid, childResult.signal);
} else {
  process.exit(childResult.exitCode);
}
