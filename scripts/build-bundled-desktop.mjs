#!/usr/bin/env node
// build-bundled-desktop.mjs — build a release desktop installer locally,
// on any of the three platforms, for either release variant:
//
//   --variant=bundled (default): the fully self-contained installer. The
//     agent payloads are baked in (repo snapshot, uv + CPython,
//     site-packages, node); the app runs the backend out of its own
//     resources.
//   --variant=light: "Hermes Light" — the remote-only client. No agent
//     payload, no payload node, no local backend; the identity overlay in
//     electron-builder.config.cjs renames the app and moves its updater
//     feed to the 'light' channel.
//
//   1. preflight: uv, git, npm exist; a release tag is resolvable
//   2. npm ci at the repo root
//   3. build ui-tui (with hermes-ink) and the dashboard SPA
//   4. npm run build in apps/desktop with the variant exported (the
//      payload runtimes — node, uv, git, gh, ripgrep — are staged there
//      by the provisioner from installation/runtime-pins.json)
//   5. npm run builder -- <platform targets>
//
// Every step always runs. There is no opt-out: a skipped step is a
// different artifact, and a different artifact is not a reproduction.
//
// Usage:
//   node scripts/build-bundled-desktop.mjs --tag=v0.20.0
//   node scripts/build-bundled-desktop.mjs --tag=v0.20.0 --variant=light
//
// Signing is CI's job (Azure/Apple secrets). Local builds are unsigned.

import { execSync, spawnSync } from "node:child_process"
import { createHash } from "node:crypto"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

import { windowsFileVersion } from "../apps/desktop/scripts/windows-file-version.mjs"

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")

const args = process.argv.slice(2)
const tagArg = args.find((a) => a.startsWith("--tag="))?.slice("--tag=".length)
const variant = args.find((a) => a.startsWith("--variant="))?.slice("--variant=".length) || "bundled"
// Everything after `--` goes to electron-builder verbatim (CI appends its
// signing configuration this way).
const dashDash = process.argv.indexOf("--")
const extraBuilderArgs = dashDash === -1 ? [] : process.argv.slice(dashDash + 1)

if (!["bundled", "light"].includes(variant)) {
  fail(`--variant must be 'bundled' or 'light', got '${variant}'`)
}

function fail(message) {
  console.error(`[build-bundled] ${message}`)
  process.exit(1)
}

function run(cmd, argv, opts = {}) {
  console.log(`[build-bundled] $ ${cmd} ${argv.join(" ")}`)
  // shell mode is for npm.cmd on Windows. It forbids arguments with
  // spaces: cmd.exe re-splits them and no quoting survives npm's own
  // re-spawn. Anything space-valued must travel as an environment
  // variable instead (see run-electron-builder.mjs for signing).
  const shell = process.platform === "win32"
  if (shell) {
    const bad = argv.find((a) => /\s/.test(a))
    if (bad) {
      fail(`argument with whitespace cannot cross the Windows shell: ${JSON.stringify(bad)} — pass it via environment instead`)
    }
  }
  const result = spawnSync(cmd, argv, { stdio: "inherit", cwd: REPO_ROOT, shell, ...opts })
  if (result.status !== 0) {
    fail(`${cmd} exited ${result.status}`)
  }
}

function capture(cmd) {
  return execSync(cmd, { cwd: REPO_ROOT, encoding: "utf8" }).trim()
}

// ── 1. preflight ────────────────────────────────────────────────────────────

for (const tool of ["uv", "git", "npm", "tar"]) {
  const probe = spawnSync(tool, ["--version"], { stdio: "ignore", shell: process.platform === "win32" })
  if (probe.status !== 0) {
    fail(`required tool missing: ${tool}`)
  }
}

// Toolchain gates. The build's output depends on these tools, so a wrong
// version makes a silently different artifact (the first Windows build
// shipped a wrong-arch uv exactly this way). The rules come from ONE
// source — package.json "engines". The EMBEDDED runtimes are a separate
// concern: they come from installation/runtime-pins.json via the
// provisioner (stageManagedRuntimes), never from the host toolchain —
// the gates below only approve the tools that BUILD the artifact (the
// JS surfaces are built and npm-installed by the host node; the payload
// interpreter is installed by the host uv). CI installs the pinned
// versions as the host toolchain, so gate == pin there by construction.
export function parseVersion(text) {
  const match = String(text).match(/(\d+)\.(\d+)\.(\d+)/)
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null
}

export function compareVersions(a, b) {
  for (let i = 0; i < 3; i += 1) {
    if (a[i] !== b[i]) return a[i] - b[i]
  }
  return 0
}

// The subset of semver ranges that package.json engines actually uses:
// space-separated comparators AND together, `||` separates alternatives.
// An unparseable comparator fails closed.
export function satisfiesRange(version, range) {
  return String(range).split("||").some((alternative) => {
    const comparators = alternative.trim().split(/\s+/).filter(Boolean)
    if (comparators.length === 0) return false
    return comparators.every((comparator) => {
      const m = comparator.match(/^(>=|<=|>|<|=)?v?(\d+)\.(\d+)\.(\d+)$/)
      if (!m) return false
      const cmp = compareVersions(version, [Number(m[2]), Number(m[3]), Number(m[4])])
      switch (m[1]) {
        case ">=": return cmp >= 0
        case "<=": return cmp <= 0
        case ">": return cmp > 0
        case "<": return cmp < 0
        default: return cmp === 0
      }
    })
  })
}

export function uvBannerProblem(banner) {
  // A build triple is three dash-joined words that end in letters
  // (aarch64-pc-windows-msvc). Its position varies: nix builds print it
  // first in the parens, official builds put a commit hash and a date
  // before it. Match it anywhere — the date (2026-07-31) cannot match
  // because its last segment is digits.
  return /[a-z0-9_]+-[a-z0-9]+-[a-z][a-z0-9-]*/.test(String(banner))
    ? null
    : "its --version prints no build triple; the payload arch guard needs one (official uv 0.12+, or any nix/source build)"
}

const engines = JSON.parse(fs.readFileSync(path.join(REPO_ROOT, "package.json"), "utf8")).engines || {}

for (const tool of ["node", "npm"]) {
  const text = tool === "node" ? process.version : capture("npm --version")
  const version = parseVersion(text)
  if (!version) {
    fail(`${tool}: cannot parse a version from ${JSON.stringify(text)}`)
  }
  const range = engines[tool]
  if (range && !satisfiesRange(version, range)) {
    fail(`${tool} ${version.join(".")} does not satisfy package.json engines ${JSON.stringify(range)} — the build would make a different artifact`)
  }
  console.log(`[build-bundled] ${tool} ${version.join(".")} (engines: ${range || "unconstrained"})`)
}

{
  const uvBanner = capture("uv --version")
  const problem = uvBannerProblem(uvBanner)
  if (problem) {
    fail(`uv (${uvBanner}) would make a broken artifact: ${problem}`)
  }
  console.log(`[build-bundled] ${uvBanner} (build-host uv; the payload uv comes from the pin table)`)
}

let tag = tagArg
if (!tag) {
  try {
    tag = capture("git describe --tags --exact-match")
  } catch {
    fail("no --tag=vX.Y.Z given and HEAD is not at an exact release tag")
  }
}
if (!/^v(?:0|[1-9]\d{0,2})\.\d+\.\d+(?:-nightly\.20\d{6}(?:\d{6})?)?$/.test(tag)) {
  fail(`'${tag}' is not a release tag (vX.Y.Z or vX.Y.0-nightly.YYYYMMDDHHMMSS)`)
}

// The canonical Hermes version is owned by pyproject.toml (the same rule
// the Nix derivation applies). electron-builder gets it via extraMetadata,
// so app.getVersion(), the artifact names, and the latest*.yml feed all
// carry the real release version instead of the UI manifest's stale one.
// The tag must agree with it: a v0.21.0 payload inside an app that
// announces 0.20.0 would make electron-updater blind to the mismatch.
//
// Nightly tags are the one exception: no version-bump commit exists (the
// tag points at plain HEAD), so the TAG is the version truth — the app
// announces v0.28.0-nightly.YYYYMMDDHHMMSS, which is what makes the nightly
// channel's semver ordering work (outversions stable 0.27.x, loses to
// stable 0.28.0).
const pyprojectVersion = fs
  .readFileSync(path.join(REPO_ROOT, "pyproject.toml"), "utf8")
  .match(/^version\s*=\s*"([^"]+)"/m)?.[1]
if (!pyprojectVersion) {
  fail("could not read version from pyproject.toml")
}
const isNightly = tag.includes("-nightly.")
if (!isNightly && tag !== `v${pyprojectVersion}`) {
  fail(`tag ${tag} does not match pyproject.toml version ${pyprojectVersion}`)
}
const artifactVersion = isNightly ? tag.slice(1) : pyprojectVersion

// Windows VERSIONINFO cannot hold a nightly's semver string: it is four
// 16-bit fields, and resedit clamps `0.28.0-nightly.20260819171926` down
// to a meaningless 0.28.0.65535. windowsFileVersion packs the nightly
// timestamp into a legal, correctly-ordering quad instead; a stable tag
// needs none of it and gets null. See apps/desktop/scripts/windows-file-version.mjs.
const fileVersion = windowsFileVersion(tag)

// On win32 the two artifacts carry DIFFERENT update stewards — nsis
// updates through electron-updater, msix through the Store — and the
// stamp is a build input (write-shell-stamp.mjs + stage-agent-payloads
// both read HERMES_PAYLOAD_UPDATE_MECHANISM). So the win leg packs
// TWICE, each pass a full top-down build with its own stamp; nothing
// ever edits a stamp after the canonical writer emits it. The second
// pass reuses the payload staging cache (.stage-cache-key ignores the
// stamp), so it costs minutes, not an hour.
const passes = {
  linux: [{ targets: "--linux AppImage", mechanism: "electron-updater" }],
  darwin: [{ targets: "--mac dmg zip", mechanism: "electron-updater" }],
  win32: [
    { targets: "--win nsis", mechanism: "electron-updater" },
    // { targets: "--win msix", mechanism: "external" },
  ],
}[process.platform]
if (!passes) {
  fail(`unsupported platform: ${process.platform}`)
}

console.log(`[build-bundled] tag=${tag} variant=${variant} platform=${process.platform}-${process.arch}`)

// ── 2-3. deps + JS surfaces ─────────────────────────────────────────────────

// ui-tui, ui-tui/packages/*, and web are npm workspaces of the repo root:
// ONE root `npm ci` installs all of them, hoisted into the root
// node_modules. Never run npm ci inside a workspace directory — that
// builds a partial shadow tree beside the hoisted one and breaks module
// resolution for the workspace builds below.
//
// The install is content-addressed: the tree npm ci produces is a pure
// function of (lockfile, node, npm, platform-arch). The stamp below
// records that tuple after a successful install; when a restored
// node_modules carries a matching stamp, the tree already IS what npm ci
// would rebuild, so rebuilding it proves nothing and is skipped. Any
// mismatch (or absent stamp) deletes the stamp first and reinstalls —
// CI's node_modules cache is an optimization this check accepts or
// rejects, never a source of truth. This matters most on the
// windows-11-arm runner image, where a cold npm ci measured 655-700s
// against 56-61s on x64 (registry tarball fetches stalling 4-6 minutes
// each), with Defender write-path exclusions confirmed applied and
// irrelevant.
const installStamp = [
  `lock=${createHash("sha256").update(fs.readFileSync(path.join(REPO_ROOT, "package-lock.json"))).digest("hex")}`,
  `node=${process.version}`,
  `npm=${execSync("npm --version", { encoding: "utf8", shell: process.platform === "win32" }).trim()}`,
  `target=${process.platform}-${process.arch}`,
].join(" ")
const installStampPath = path.join(REPO_ROOT, "node_modules", ".install-stamp")
if (fs.existsSync(installStampPath) && fs.readFileSync(installStampPath, "utf8") === installStamp) {
  console.log("[build-bundled] node_modules matches its install stamp — npm ci output already present")
} else {
  fs.rmSync(installStampPath, { force: true })
  run("npm", ["ci", "--no-audit", "--no-fund", "--fetch-retries=5"], {
    env: {
      ...process.env, // spawnSync env REPLACES the child environment; keep PATH etc.
      "CI": "true" // skip annoying unicode install banner
    }
  })
  fs.writeFileSync(installStampPath, installStamp)
}
run("npm", ["run", "build", "--workspace", "ui-tui"])
run("npm", ["run", "build", "--workspace", "web"])

// The payload node is NOT downloaded here: it is a managed runtime from
// installation/runtime-pins.json, staged by the provisioner inside
// stage-agent-payloads.mjs (stageManagedRuntimes) with its pinned URL
// and sha256. The engines gate above still approves the HOST node that
// builds the JS surfaces; tests/test_engines_satisfiable.py holds the
// pinned node/npm inside those same ranges, so the surfaces the host
// builds are the surfaces the pinned runtime can run.

// ── 5-6. desktop build + package ────────────────────────────────────────────

const env = {
  ...process.env,
  HERMES_DESKTOP_VARIANT: variant,
  HERMES_PAYLOAD_TAG: tag,
}

const desktop = path.join(REPO_ROOT, "apps", "desktop")

for (const pass of passes) {
  const passEnv = { ...env, HERMES_PAYLOAD_UPDATE_MECHANISM: pass.mechanism }
  console.log(`[build-bundled] pass: ${pass.targets} (updateMechanism=${pass.mechanism})`)
  run("npm", ["run", "build"], { cwd: desktop, env: passEnv })
  run(
    "npm",
    [
      "run", "builder", "--",
      ...pass.targets.split(" "),
      `-c.extraMetadata.version=${artifactVersion}`,
      // Both keys or neither: app-builder-lib reads shortVersion for the
      // VERSIONINFO FileVersion and shortVersionWindows for ProductVersion,
      // and NsisTarget gates the uninstaller's VIProductVersion on
      // shortVersion being set while reading shortVersionWindows for the
      // value — setting only one emits `-XVIProductVersion undefined`.
      ...(fileVersion
        ? [
            `-c.extraMetadata.shortVersion=${fileVersion}`,
            `-c.extraMetadata.shortVersionWindows=${fileVersion}`,
          ]
        : []),
      ...extraBuilderArgs,
    ],
    { cwd: desktop, env: passEnv }
  )
}
console.log(`[build-bundled] artifacts: ${path.join(desktop, "release")}`)
