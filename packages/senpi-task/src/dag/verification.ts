// Evidence-first verification records: every required check leaves a durable,
// reproducible record, and the acceptance gate reads only those records.
// A worker's assertion is never evidence; only a genuine execution record counts.
import { spawn } from "node:child_process"
import { isAbsolute, resolve } from "node:path"

import type { DagRunId } from "./types"

export const DAG_VERIFICATION_STATUSES = [
  "pending",
  "running",
  "passed",
  "failed",
  "timed_out",
  "cancelled",
  "unavailable",
  "skipped",
  "stale",
] as const

export type DagVerificationStatus = (typeof DAG_VERIFICATION_STATUSES)[number]

// Only `passed` accepts. Every other status — including `skipped` and `stale` —
// blocks acceptance. There is no third outcome.
export function isVerificationAccepting(status: DagVerificationStatus): boolean {
  return status === "passed"
}

export const DAG_VERIFY_CHECK_CATEGORIES = [
  "unit",
  "integration",
  "typecheck",
  "lint",
  "build",
  "regression",
  "security",
  "schema",
  "session",
  "native",
] as const

export type DagVerifyCheckCategory = (typeof DAG_VERIFY_CHECK_CATEGORIES)[number]

// One check the engine must execute and record. `command` is an argv array, never a
// shell string, so no shell injection surface exists. `coveredPaths` scopes freshness:
// a repair touching these paths invalidates this record; empty means run-scoped.
export type DagVerifyCheck = {
  readonly checkId: string
  readonly category: DagVerifyCheckCategory
  readonly command: readonly string[]
  readonly cwd?: string
  readonly timeoutMs: number
  readonly criteria: readonly string[]
  readonly coveredPaths?: readonly string[]
  readonly idempotent?: boolean
}

export type DagVerifyCommandResult = {
  readonly started: boolean
  readonly exitCode?: number
  readonly signal?: string
  readonly timedOut: boolean
  readonly cancelled: boolean
  readonly output: string
  readonly outputBytes: number
  readonly durationMs: number
  // True when the command may have changed state before it stopped (timeout or
  // kill after start on a non-idempotent check). A retry must reconcile first.
  readonly uncertainEffect: boolean
}

export type DagVerifyRunnerInput = {
  readonly check: DagVerifyCheck
  readonly projectDir: string
  readonly attempt: number
  readonly isCancelled?: () => boolean
}

export type DagVerifyRunner = (input: DagVerifyRunnerInput) => Promise<DagVerifyCommandResult>

// Failure vocabulary shared by the gate and the repair controller. `unknown`
// blocks acceptance exactly like a product defect; it never degrades to noise.
export const DAG_FAILURE_CLASSIFICATIONS = [
  "product_defect",
  "environment",
  "provider",
  "timeout",
  "flaky",
  "invalid_contract",
  "malformed_result",
  "unknown",
] as const

export type DagFailureClassification = (typeof DAG_FAILURE_CLASSIFICATIONS)[number]

// Durable per-check evidence. Written once per attempt, never mutated: a new
// attempt writes a new record, so history survives and duplicates cannot
// overwrite success or failure.
export type DagVerificationRecord = {
  readonly schemaVersion: 1
  readonly runId: string
  readonly parentSessionId: string
  readonly rootSessionId: string
  readonly nodeId?: string
  readonly workerAttempt?: number
  readonly checkId: string
  readonly category: DagVerifyCheckCategory
  readonly criteria: readonly string[]
  readonly command: readonly string[]
  readonly cwd: string
  readonly projectDir: string
  readonly timeoutMs: number
  readonly sourceRevision: string
  readonly nodeFingerprint?: string
  readonly generation: number
  readonly repairAttempt: number
  readonly startedAt: string
  readonly completedAt: string
  readonly durationMs: number
  readonly started: boolean
  readonly exitCode?: number
  readonly signal?: string
  readonly timedOut: boolean
  readonly cancelled: boolean
  readonly outputSummary: string
  readonly outputBytes: number
  readonly coveredPaths: readonly string[]
  readonly uncertainEffect: boolean
  readonly diagnosticRef?: string
  readonly status: DagVerificationStatus
  readonly classification?: DagFailureClassification
}

export type DagVerificationRecordInput = {
  readonly runId: string
  readonly parentSessionId: string
  readonly rootSessionId: string
  readonly nodeId?: string
  readonly workerAttempt?: number
  readonly check: DagVerifyCheck
  readonly cwd: string
  readonly projectDir: string
  readonly sourceRevision: string
  readonly nodeFingerprint?: string
  readonly generation: number
  readonly repairAttempt: number
  readonly result: DagVerifyCommandResult
}

const OUTPUT_SUMMARY_CHARS = 4000

// A `passed` status demands a genuine execution record: started, finished without
// timeout or cancellation, exit code zero. Anything else degrades to the honest
// status instead of inheriting the runner's claim.
export function deriveVerificationStatus(result: DagVerifyCommandResult): DagVerificationStatus {
  if (result.cancelled) return "cancelled"
  if (result.timedOut) return "timed_out"
  if (!result.started) return "unavailable"
  if (result.exitCode === undefined) return "failed"
  return result.exitCode === 0 ? "passed" : "failed"
}

export function buildVerificationRecord(input: DagVerificationRecordInput): DagVerificationRecord {
  const now = new Date().toISOString()
  const startedAt = now
  const record: DagVerificationRecord = {
    schemaVersion: 1,
    runId: input.runId,
    parentSessionId: input.parentSessionId,
    rootSessionId: input.rootSessionId,
    checkId: input.check.checkId,
    category: input.check.category,
    criteria: [...input.check.criteria],
    command: [...input.check.command],
    cwd: input.cwd,
    projectDir: input.projectDir,
    timeoutMs: input.check.timeoutMs,
    sourceRevision: input.sourceRevision,
    generation: input.generation,
    repairAttempt: input.repairAttempt,
    startedAt,
    completedAt: now,
    durationMs: input.result.durationMs,
    started: input.result.started,
    timedOut: input.result.timedOut,
    cancelled: input.result.cancelled,
    outputSummary: scrubSecretValues(input.result.output.slice(0, OUTPUT_SUMMARY_CHARS)),
    outputBytes: input.result.outputBytes,
    coveredPaths: input.check.coveredPaths === undefined ? [] : [...input.check.coveredPaths],
    uncertainEffect: input.result.uncertainEffect,
    status: deriveVerificationStatus(input.result),
  }
  const withCompletion: DagVerificationRecord = withOptionalCompletion(record, input)
  if (input.nodeId !== undefined) return { ...withCompletion, nodeId: input.nodeId }
  return withCompletion
}

function withOptionalCompletion(
  record: DagVerificationRecord,
  input: DagVerificationRecordInput,
): DagVerificationRecord {
  let next = record
  if (input.result.exitCode !== undefined) next = { ...next, exitCode: input.result.exitCode }
  if (input.result.signal !== undefined) next = { ...next, signal: input.result.signal }
  if (input.nodeFingerprint !== undefined) next = { ...next, nodeFingerprint: input.nodeFingerprint }
  if (input.workerAttempt !== undefined) next = { ...next, workerAttempt: input.workerAttempt }
  return next
}

export function withRecordClassification(
  record: DagVerificationRecord,
  classification: DagFailureClassification,
  diagnosticRef?: string,
): DagVerificationRecord {
  const classified: DagVerificationRecord = { ...record, classification }
  if (diagnosticRef === undefined) return classified
  return { ...classified, diagnosticRef }
}

export type DagVerificationFreshness =
  | { readonly fresh: true }
  | { readonly fresh: false; readonly reason: string }

export type DagVerificationCurrent = {
  readonly sourceRevision: string
  readonly nodeFingerprints: ReadonlyMap<string, string>
  readonly generation: number
}

// Freshness is provenance, not timestamps: a record counts only when it names
// the current source revision and node fingerprint, belongs to the latest
// attempt, and holds a genuine execution. An unrelated file change that leaves
// the fingerprints intact keeps the record valid.
export function checkVerificationFreshness(
  record: DagVerificationRecord,
  current: DagVerificationCurrent,
  latestAttempt: number,
): DagVerificationFreshness {
  if (record.schemaVersion !== 1) return { fresh: false, reason: "unsupported record schema" }
  if (record.sourceRevision !== current.sourceRevision) {
    return { fresh: false, reason: "source revision changed" }
  }
  if (record.nodeId !== undefined) {
    const fingerprint = current.nodeFingerprints.get(record.nodeId)
    if (fingerprint === undefined) return { fresh: false, reason: "node no longer in definition" }
    if (record.nodeFingerprint !== fingerprint) return { fresh: false, reason: "node definition changed" }
  }
  if (record.generation !== current.generation) {
    return { fresh: false, reason: "plan revision changed" }
  }
  if (record.repairAttempt !== latestAttempt) return { fresh: false, reason: "superseded by a newer attempt" }
  if (record.status === "pending" || record.status === "running") {
    return { fresh: false, reason: "check has not finished" }
  }
  if (record.status === "passed" && (!record.started || record.exitCode === undefined)) {
    return { fresh: false, reason: "passed without a genuine execution record" }
  }
  return { fresh: true }
}

// A repair that writes files invalidates every record whose covered paths
// intersect the change. Records with no covered paths are run-scoped and
// survive file-local repairs.
export function verificationAffectedByRepair(
  record: DagVerificationRecord,
  changedPaths: readonly string[],
): boolean {
  const covered = verificationCoveredPaths(record)
  if (covered.length === 0) return false
  return changedPaths.some((changed) => covered.some((path) => pathsIntersect(path, changed)))
}

function verificationCoveredPaths(record: DagVerificationRecord): readonly string[] {
  return record.coveredPaths
}

function pathsIntersect(covered: string, changed: string): boolean {
  return covered === changed || changed.startsWith(covered + "/") || covered.startsWith(changed + "/")
}

// Selection: which checks a change scope requires, and why each omitted check
// stays out. The engine consumes this; a list in a prompt is not a plan.
export type DagChangeScope = {
  readonly changedFiles: readonly string[]
  readonly touchesScheduler: boolean
  readonly touchesCompletionGate: boolean
  readonly touchesWorkerLifecycle: boolean
  readonly touchesPersistedState: boolean
  readonly isDocsOnly: boolean
}

export type DagSelectedChecks = {
  readonly required: readonly DagVerifyCheck[]
  readonly omitted: readonly { readonly check: DagVerifyCheck; readonly reason: string }[]
}

export function selectVerificationChecks(
  scope: DagChangeScope,
  available: readonly DagVerifyCheck[],
): DagSelectedChecks {
  const required: DagVerifyCheck[] = []
  const omitted: Array<{ readonly check: DagVerifyCheck; readonly reason: string }> = []
  const wants = (check: DagVerifyCheck): string | null => {
    if (scope.isDocsOnly && (check.category === "unit" || check.category === "integration")) {
      return "docs-only change carries no executable behavior"
    }
    if (check.category === "native" && scope.touchesWorkerLifecycle) return null
    if (check.category === "session" && scope.touchesWorkerLifecycle) return null
    if (check.category === "schema" && scope.touchesPersistedState) return null
    if (check.category === "build" && (scope.touchesScheduler || scope.touchesCompletionGate)) return null
    if (check.category === "typecheck" || check.category === "lint") return null
    if (check.category === "security") return null
    if (check.category === "regression" && (scope.touchesScheduler || scope.touchesCompletionGate)) return null
    if (check.category === "unit" || check.category === "integration") return null
    return "no selection rule covers this category for this scope"
  }
  for (const check of available) {
    const reason = wants(check)
    if (reason === null) required.push(check)
    else omitted.push({ check, reason })
  }
  return { required, omitted }
}

// Fail-closed acceptance: accepted only when every required check has a fresh,
// genuine `passed` record. Missing, stale, malformed, or non-passed evidence
// blocks with a named reason. No evidence ever defaults to acceptance.
export type DagAcceptanceVerdict = {
  readonly accepted: boolean
  readonly reasons: readonly string[]
}

export type DagAcceptanceRequirement = {
  readonly check: DagVerifyCheck
  readonly nodeId?: string
}

export type DagAcceptanceInput = {
  readonly requirements: readonly DagAcceptanceRequirement[]
  readonly records: ReadonlyMap<string, DagVerificationRecord>
  readonly current: DagVerificationCurrent
  readonly latestAttempts: ReadonlyMap<string, number>
}

export function verificationRecordKey(nodeId: string | undefined, checkId: string): string {
  return nodeId === undefined ? checkId : `${nodeId}/${checkId}`
}

export function evaluateAcceptance(input: DagAcceptanceInput): DagAcceptanceVerdict {
  const reasons: string[] = []
  for (const requirement of input.requirements) {
    const key = verificationRecordKey(requirement.nodeId, requirement.check.checkId)
    const record = input.records.get(key)
    if (record === undefined) {
      reasons.push(`missing verification record for ${key}`)
      continue
    }
    const latest = input.latestAttempts.get(key) ?? record.repairAttempt
    const freshness = checkVerificationFreshness(record, input.current, latest)
    if (!freshness.fresh) {
      reasons.push(`stale verification record for ${key}: ${freshness.reason}`)
      continue
    }
    if (!isVerificationAccepting(record.status)) {
      reasons.push(`check ${key} holds status ${record.status}, not passed`)
      continue
    }
    if (record.classification === "invalid_contract") {
      reasons.push(`check ${key} test contract is under review, not trusted`)
    }
  }
  if (reasons.length > 0) return { accepted: false, reasons }
  return { accepted: true, reasons: [] }
}

export type VerificationRecordStore = {
  readonly writeVerification: (runId: DagRunId, nodeId: string, checkId: string, attempt: number, record: string) => string
  readonly readVerification: (runId: DagRunId, nodeId: string, checkId: string, attempt: number) => string | null
  readonly listVerifications: (runId: DagRunId) => readonly string[]
}

export function serializeVerificationRecord(record: DagVerificationRecord): string {
  return JSON.stringify(record)
}

export function parseVerificationRecord(text: string): DagVerificationRecord | null {
  let value: unknown
  try {
    // SAFETY: JSON boundary; the result is validated field-by-field below before use.
    value = JSON.parse(text) as unknown
  } catch {
    return null
  }
  if (value === null || Array.isArray(value)) return null
  // SAFETY: envelope fields checked below (schemaVersion literal plus known status) before trust.
  const envelope = value as { readonly schemaVersion?: unknown; readonly status?: unknown }
  if (envelope.schemaVersion !== 1) return null
  const statuses: readonly unknown[] = DAG_VERIFICATION_STATUSES
  if (!statuses.includes(envelope.status)) return null
  // SAFETY: envelope validated above; remaining fields ride as evidence and the
  // freshness gate re-checks provenance before acceptance.
  return value as DagVerificationRecord
}

export type RunVerificationRecords = {
  readonly records: ReadonlyMap<string, DagVerificationRecord>
  readonly malformed: readonly string[]
}

// Reads every persisted record for a run. Malformed files are reported, never
// trusted: a result that cannot be parsed cannot accept anything.
export function readRunVerificationRecords(
  store: VerificationRecordStore,
  runId: DagRunId,
): RunVerificationRecords {
  const records = new Map<string, DagVerificationRecord>()
  const malformed: string[] = []
  for (const file of store.listVerifications(runId)) {
    const text = readListedVerification(store, runId, file)
    if (text === null) continue
    const record = parseVerificationRecord(text)
    if (record === null) {
      malformed.push(file)
      continue
    }
    records.set(verificationRecordKey(record.nodeId, record.checkId), record)
  }
  return { records, malformed }
}

function readListedVerification(
  store: VerificationRecordStore,
  runId: DagRunId,
  file: string,
): string | null {
  const suffix = ".json"
  if (!file.endsWith(suffix)) return null
  const stem = file.slice(0, -suffix.length)
  const attemptSeparator = stem.lastIndexOf(".")
  if (attemptSeparator < 0) return null
  const head = stem.slice(0, attemptSeparator)
  const checkSeparator = head.lastIndexOf(".")
  if (checkSeparator < 0) return null
  const nodeId = head.slice(0, checkSeparator)
  const checkId = head.slice(checkSeparator + 1)
  const attempt = Number(stem.slice(attemptSeparator + 1))
  if (!Number.isInteger(attempt) || attempt < 0) return null
  return store.readVerification(runId, nodeId, checkId, attempt)
}

// Recovery reconciliation: a restart never assumes an unrecorded result passed.
// Records left in a non-terminal state by the crash are reported, never trusted.
export function reconcileInterruptedVerifications(
  records: readonly DagVerificationRecord[],
): readonly DagVerificationRecord[] {
  return records.filter(
    (record) => record.status === "pending" || record.status === "running",
  )
}

// Secret scrubbing for persisted diagnostics. Key-based redaction (see
// store/redaction.ts) covers structured payloads; command output is free text,
// so values after a sensitive name are masked in place. The shape of the
// failure stays; the credential does not.
const SECRET_VALUE_PATTERN =
  /(api[_-]?key|token|secret|password|passwd|authorization|bearer|session[_-]?key)\s*[:=]\s*("([^"]+)"|'([^']+)'|(\S+))/gi

export function scrubSecretValues(text: string): string {
  return text.replace(SECRET_VALUE_PATTERN, (_match, name, _value, dq, sq, bare) => {
    const kept = dq !== undefined ? `"${dq.slice(0, 1)}...${dq.slice(-1)}"` : sq !== undefined ? `'${sq.slice(0, 1)}...${sq.slice(-1)}'` : `${String(bare).slice(0, 1)}...`
    return `${name}=[REDACTED:${kept}]`
  })
}

// Default production runner: argv execution with timeout kill, output cap, and
// a cwd locked inside the project dir. Never a shell string, never ambient env
// persisted, never unbounded output.
export const VERIFY_RUNNER_OUTPUT_LIMIT_BYTES = 256 * 1024

export async function runVerificationCommand(input: DagVerifyRunnerInput): Promise<DagVerifyCommandResult> {
  const startedAt = Date.now()
  if (input.isCancelled?.() === true) {
    return {
      started: false,
      timedOut: false,
      cancelled: true,
      output: "",
      outputBytes: 0,
      durationMs: Date.now() - startedAt,
      uncertainEffect: false,
    }
  }
  const cwd = resolveCwd(input.projectDir, input.check.cwd)
  if (cwd === null) {
    return {
      started: false,
      timedOut: false,
      cancelled: false,
      output: `refused: check cwd escapes the project dir`,
      outputBytes: 0,
      durationMs: Date.now() - startedAt,
      uncertainEffect: false,
    }
  }
  const command = [...input.check.command]
  const executable = command.shift()
  if (executable === undefined || executable === "") {
    return {
      started: false,
      timedOut: false,
      cancelled: false,
      output: "refused: empty command",
      outputBytes: 0,
      durationMs: Date.now() - startedAt,
      uncertainEffect: false,
    }
  }
  return runSpawn(executable, command, cwd, input.check.timeoutMs, startedAt, input)
}

function resolveCwd(projectDir: string, cwd: string | undefined): string | null {
  if (cwd === undefined) return projectDir
  if (isAbsolute(cwd)) {
    const absolute = resolve(cwd)
    return absolute === projectDir || absolute.startsWith(projectDir + "/") ? absolute : null
  }
  const absolute = resolve(projectDir, cwd)
  return absolute === projectDir || absolute.startsWith(projectDir + "/") ? absolute : null
}

async function runSpawn(
  executable: string,
  args: readonly string[],
  cwd: string,
  timeoutMs: number,
  startedAt: number,
  input: DagVerifyRunnerInput,
): Promise<DagVerifyCommandResult> {
  let child: ReturnType<typeof spawn> | null = null
  try {
    child = spawn(executable, [...args], { cwd, stdio: ["ignore", "pipe", "pipe"] })
  } catch {
    return {
      started: false,
      timedOut: false,
      cancelled: false,
      output: `cannot start: executable not found: ${executable}`,
      outputBytes: 0,
      durationMs: Date.now() - startedAt,
      uncertainEffect: false,
    }
  }
  const chunks: Buffer[] = []
  let bytes = 0
  const take = (data: Buffer): void => {
    if (bytes >= VERIFY_RUNNER_OUTPUT_LIMIT_BYTES) return
    const room = VERIFY_RUNNER_OUTPUT_LIMIT_BYTES - bytes
    const slice = data.length > room ? data.subarray(0, room) : data
    chunks.push(slice)
    bytes += slice.length
  }
  child.stdout?.on("data", take)
  child.stderr?.on("data", take)
  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    child?.kill("SIGKILL")
  }, Math.max(1, timeoutMs))
  timer.unref?.()
  const outcome = await new Promise<{ readonly code: number | null; readonly signal: string | null }>(
    (resolveOutcome) => {
      child?.on("error", () => resolveOutcome({ code: null, signal: "spawn_error" }))
      child?.on("close", (code, signal) => resolveOutcome({ code, signal: signal ?? null }))
    },
  )
  clearTimeout(timer)
  const cancelled = input.isCancelled?.() === true
  const output = scrubSecretValues(Buffer.concat(chunks).toString("utf8"))
  const idempotent = input.check.idempotent === true
  return {
    started: true,
    exitCode: outcome.code ?? undefined,
    signal: outcome.signal ?? undefined,
    timedOut,
    cancelled,
    output,
    outputBytes: bytes,
    durationMs: Date.now() - startedAt,
    uncertainEffect: (timedOut || outcome.signal !== null) && !idempotent,
  }
}
