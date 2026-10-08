# Free-channel install (arm's-length free download)

This page covers the arm's-length free channel for the free OmO-derived plugin.
It is the only supported free-download path. It points at fork releases, has
no paywall, and puts no login wall on OmO-derived functionality.

- Free channel source: `https://github.com/dang232/oh-my-openAlcore/releases`
- Upstream project: `https://github.com/code-yeongyu/oh-my-openagent`
- Branch for this packaging: `ohmy-ide-wave2` (base `6e0428c195d560dbaf9aa3ea891ad1025956d545`)
- Paid Alwork IDE slot stays empty: no plugin bits are bundled into any paid path.

## What the free-download payload is

The free-download payload is the fork source at the `ohmy-ide-wave2` branch
plus the npm `files[]` payload defined in the root `package.json`. The payload
always ships these notice files:

- `THIRD-PARTY-NOTICES.md`
- `packages/omo-codex/THIRD-PARTY-NOTICES.md`

License files are never swapped by this channel. `LICENSE.md` stays
byte-identical to upstream. The expected prefix is `b61ac928`.

Verify the shipped payload before installing:

```bash
node scripts/check-third-party-notices.mjs
node scripts/check-third-party-notices.mjs --ship
```

`--ship` checks `package.json` `files[]` and the `npm pack --dry-run` output.
If a notice file is missing from the payload, the command exits non-zero and
names the missing path. It never half-installs anything.

## Download

Pick a fork release tarball from the free channel and verify it. Do not use a
paid Alwork artifact for this plugin. The paid slot is intentionally empty.

```powershell
# Windows (PowerShell) example shape; replace <version> with the fork release tag
Invoke-WebRequest -Uri "https://github.com/dang232/oh-my-openAlcore/archive/refs/tags/<version>.zip" -OutFile "oh-my-openAlcore-<version>.zip"
certutil -hashfile "oh-my-openAlcore-<version>.zip" SHA256
```

```bash
# macOS / Linux / WSL example shape
curl -fsSL -o "oh-my-openAlcore-<version>.tar.gz" "https://github.com/dang232/oh-my-openAlcore/archive/refs/tags/<version>.tar.gz"
sha256sum "oh-my-openAlcore-<version>.tar.gz"
```

A broken URL fails loudly. `curl -fsSL` exits non-zero on HTTP errors and
writes no usable archive. `certutil` / `sha256sum` mismatch means stop: delete
the download and re-fetch. Never unpack a file whose hash did not verify.

## Install with no subscription and no Alcore account

Standalone install needs no subscription and no Alcore account. It also needs
no paid Alwork service. Run from the unpacked fork source:

```bash
node scripts/check-third-party-notices.mjs
node --test scripts/check-third-party-notices.test.mjs
```

Expected standalone outputs:

- `THIRD-PARTY-NOTICES.md: <N> required notice entries present`
- `node --test` passes with exit code 0.

Confirm notices are byte-identical to upstream before running:

```powershell
certutil -hashfile LICENSE.md SHA256
```

The SHA256 must start with `b61ac928`. `THIRD-PARTY-NOTICES.md` must exist.
If either check fails, stop and re-fetch the free channel payload.

The postinstall step never blocks on accounts. It prints the rename notice
exactly once and always exits 0, even when platform binaries do not resolve.
Covered by `postinstall.test.ts`:

```bash
bun test postinstall.test.ts --timeout 20000
```

## Run standalone

OmO-derived functionality in this channel runs without a subscription and
without an Alcore account. No login gate stands in front of it.

```bash
node scripts/check-third-party-notices.mjs
bun test postinstall.test.ts --timeout 20000
```

Both commands run in an isolated home directory in CI style checks and touch
no real `~/.codex`, `~/.senpi/agent`, or Alwork paid state. If a command
reports a missing notice entry or a non-zero exit, the failure is loud and the
install is refused. There is no partial plugin registration.

## No paywall, no login wall

- No paywall: the free channel serves fork releases. No checkout, no
  entitlement check, and no subscription flag gates OmO-derived behavior.
- No login wall: no Alcore account, token, or session is required to install
  or run the free plugin. Commands above run with a clean environment.
- Arm's length: the free plugin never bundles paid Alwork artifacts, and the
  paid Alwork IDE slot never bundles plugin bits. The slot-empty proof lives
  in `.omo/evidence/task-11-project-ide.md`.

## Troubleshooting a broken download

- HTTP error: `curl -fsSL` prints the HTTP failure and exits non-zero. No
  archive is left behind in usable form. Delete any partial file and retry.
- Hash mismatch: delete the file. Do not unpack it. Re-fetch from the fork
  releases URL above.
- Missing notice: `node scripts/check-third-party-notices.mjs` names the
  missing entry and exits 1. Re-fetch the payload; do not proceed.
- Installer confusion with paid builds: stop. The paid Alwork tree is
  read-only for this channel. File a report instead of copying plugin files
  into it.
