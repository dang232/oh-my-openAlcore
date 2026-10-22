// Bounded diagnosis and repair: classify failures from process evidence, enforce
// finite budgets, re-run the failed check, then the regression checks, and
// re-evaluate the gate. Every attempt is recorded; identical failures stop.
import {
  buildVerificationRecord,
  checkVerificationFreshness,
  evaluateAcceptance,
  verificationRecordKey,
  withRecordClassification,
  type DagAcceptanceRequirement,
  type DagFailureClassification,
  type DagVerificationRecord,
  type DagVerifyCheck,
  type DagVerifyCommandResult,
  type DagVerifyRunner,
} from "./verification"

export type DagRepairBudgets = {
  readonly maxAttemptsPerCheck: number
  readonly maxAttemptsPerNode: number
  readonly maxAttemptsPerRun: number
  readonly maxIdenticalFailures: number
}

export const DAG_REPAIR_BUDGET_DEFAULTS: DagRepairBudgets = {
  maxAttemptsPerCheck: 2,
  maxAttemptsPerNode: 3,
  maxAttemptsPerRun: 5,
  maxIdenticalFailures: 2,
}

// Stable identity of a failure for loop detection: classification plus exit
// signal plus the scrubbed tail of the output. Two failures share a signature
// only when the redacted evidence matches, so a genuinely new error re-arms.
export function failureSignature(
  classification: DagFailureClassification,
  result: { readonly exitCode?: number; readonly signal?: string; readonly timedOut?: boolean; readonly output: string },
): string {
  const tail = result.output.slice(-500)
  return [classification, String(result.exitCode), result.signal ?? "", String(result.timedOut), tail].join("|")
}

export type DagRepairAttempt = {
  readonly check: DagVerifyCheck
  readonly nodeId?: string
  readonly attempt: number
  readonly changedFiles: readonly string[]
}

// What the controller may do next. `recheck` re-runs the same check (the worker
// output stands; only the evidence was missing, stale, or environmental).
// `renode` asks the owner to re-run the worker through the existing node retry
// path before re-verifying. `blocked` and `exhausted` both refuse further
// action; they differ only in whether budget remains for a different strategy.
export type DagRepairDecision =
  | { readonly kind: "recheck"; readonly attempt: DagRepairAttempt }
  | { readonly kind: "renode"; readonly attempt: DagRepairAttempt; readonly reason: string }
  | { readonly kind: "blocked"; readonly reason: string }
  | { readonly kind: "exhausted"; readonly reason: string }

export type DagRepairState = {
  readonly budgets: DagRepairBudgets
  readonly attemptsPerCheck: ReadonlyMap<string, number>
  readonly attemptsPerNode: ReadonlyMap<string, number>
  readonly attemptsPerRun: number
  readonly signatures: ReadonlyMap<string, number>
}

export function emptyRepairState(budgets: DagRepairBudgets = DAG_REPAIR_BUDGET_DEFAULTS): DagRepairState {
  return {
    budgets,
    attemptsPerCheck: new Map(),
    attemptsPerNode: new Map(),
    attemptsPerRun: 0,
    signatures: new Map(),
  }
}

export type DagRepairRequest = {
  readonly record: DagVerificationRecord
  readonly signature: string
  readonly changedFiles: readonly string[]
}

export function decideRepair(state: DagRepairState, request: DagRepairRequest): DagRepairDecision {
  const checkKey = request.record.checkId
  const nodeKey = request.record.nodeId ?? ""
  const checkAttempts = state.attemptsPerCheck.get(checkKey) ?? 0
  const nodeAttempts = nodeKey === "" ? 0 : (state.attemptsPerNode.get(nodeKey) ?? 0)
  if (state.attemptsPerRun >= state.budgets.maxAttemptsPerRun) {
    return { kind: "exhausted", reason: `run repair budget spent (${state.attemptsPerRun}/${state.budgets.maxAttemptsPerRun})` }
  }
  if (checkAttempts >= state.budgets.maxAttemptsPerCheck) {
    return { kind: "exhausted", reason: `check ${checkKey} repair budget spent (${checkAttempts}/${state.budgets.maxAttemptsPerCheck})` }
  }
  if (nodeKey !== "" && nodeAttempts >= state.budgets.maxAttemptsPerNode) {
    return { kind: "exhausted", reason: `node repair budget spent (${nodeAttempts}/${state.budgets.maxAttemptsPerNode})` }
  }
  const identical = state.signatures.get(request.signature) ?? 0
  if (identical >= state.budgets.maxIdenticalFailures) {
    return {
      kind: "blocked",
      reason: `identical failure repeated ${identical} times without new evidence; refusing the same strategy`,
    }
  }
  const classification = request.record.classification ?? "unknown"
  if (classification === "invalid_contract") {
    return { kind: "blocked", reason: "test contract diagnosed invalid; needs review, never auto-weakened" }
  }
  if (classification === "provider") {
    return { kind: "blocked", reason: "provider unavailable; live execution stays unverified" }
  }
  if (request.record.uncertainEffect) {
    return { kind: "blocked", reason: "uncertain side effect; reconcile before any retry" }
  }
  const attempt: DagRepairAttempt = {
    check: rebuildCheck(request.record),
    attempt: request.record.repairAttempt + 1,
    changedFiles: request.changedFiles,
  }
  const withNode: DagRepairAttempt =
    request.record.nodeId === undefined ? attempt : { ...attempt, nodeId: request.record.nodeId }
  if (classification === "product_defect" || classification === "unknown") {
    return { kind: "renode", attempt: withNode, reason: `check evidence points at the worker output (${classification})` }
  }
  return { kind: "recheck", attempt: withNode }
}

function rebuildCheck(record: DagVerificationRecord): DagVerifyCheck {
  const check: DagVerifyCheck = {
    checkId: record.checkId,
    category: record.category,
    command: [...record.command],
    timeoutMs: record.timeoutMs,
    criteria: [...record.criteria],
    coveredPaths: [...record.coveredPaths],
  }
  if (record.cwd !== record.projectDir) return { ...check, cwd: record.cwd }
  return check
}

export function recordRepairAttempt(state: DagRepairState, request: DagRepairRequest): DagRepairState {
  const checkKey = request.record.checkId
  const nodeKey = request.record.nodeId ?? ""
  const attemptsPerCheck = new Map(state.attemptsPerCheck)
  attemptsPerCheck.set(checkKey, (attemptsPerCheck.get(checkKey) ?? 0) + 1)
  const attemptsPerNode = new Map(state.attemptsPerNode)
  if (nodeKey !== "") attemptsPerNode.set(nodeKey, (attemptsPerNode.get(nodeKey) ?? 0) + 1)
  const signatures = new Map(state.signatures)
  signatures.set(request.signature, (signatures.get(request.signature) ?? 0) + 1)
  return {
    budgets: state.budgets,
    attemptsPerCheck,
    attemptsPerNode,
    attemptsPerRun: state.attemptsPerRun + 1,
    signatures,
  }
}

// Classify from process evidence, never from a worker statement. A zero exit
// with a missing test run is not success here; the caller marks that through
// the verification status, and this stays on the process facts.
export function classifyCommandResult(result: DagVerifyCommandResult): DagFailureClassification {
  if (result.cancelled) return "unknown"
  if (result.timedOut) return "timeout"
  if (!result.started) return "environment"
  if (result.exitCode === undefined) return "malformed_result"
  if (result.exitCode === 0) return "unknown"
  if (result.signal !== undefined && result.signal !== "") return "timeout"
  if (looksLikeMissingDependency(result.output)) return "environment"
  if (looksLikeProviderFailure(result.output)) return "provider"
  return "product_defect"
}

const MISSING_DEPENDENCY_PATTERN = /cannot find (module|package)|command not found|ENOENT|no such file/i
const PROVIDER_PATTERN = /ENOTFOUND|EAI_AGAIN|ETIMEDOUT|ECONNREFUSED|DNS|network is unreachable|provider/i

function looksLikeMissingDependency(output: string): boolean {
  return MISSING_DEPENDENCY_PATTERN.test(output)
}

function looksLikeProviderFailure(output: string): boolean {
  return PROVIDER_PATTERN.test(output)
}

export type DagRepairOutcome =
  | { readonly kind: "accepted"; readonly attempts: number }
  | { readonly kind: "blocked"; readonly reasons: readonly string[] }

// Owner-driven repair loop: the production repair path. Re-runs the failed
// check through the injected runner, re-runs the regression set, and
// re-evaluates the gate after every attempt. `retryNode` delegates worker
// re-execution to the existing node retry path; the controller never spawns
// workers itself. Budgets bound every loop; the gate has the last word.
export type RepairDagRunInput = {
  readonly runner: DagVerifyRunner
  readonly projectDir: string
  readonly sourceRevision: string
  readonly generation: number
  readonly budgets?: DagRepairBudgets
  readonly requirements: readonly DagAcceptanceRequirement[]
  readonly regressionChecks: readonly DagVerifyCheck[]
  readonly records: ReadonlyMap<string, DagVerificationRecord>
  readonly currentFingerprints: ReadonlyMap<string, string>
  readonly changedFiles: readonly string[]
  readonly isCancelled?: () => boolean
  readonly retryNode?: (nodeId: string) => Promise<void>
  readonly persistRecord?: (record: DagVerificationRecord) => void
}

export async function repairDagRun(input: RepairDagRunInput): Promise<DagRepairOutcome> {
  let state = emptyRepairState(input.budgets)
  const records = new Map(input.records)
  const latestAttempts = new Map<string, number>()
  for (const [key, record] of records) latestAttempts.set(key, record.repairAttempt)
  let attempts = 0
  for (;;) {
    if (input.isCancelled?.() === true) return { kind: "blocked", reasons: ["cancelled during repair"] }
    const verdict = evaluateAcceptance({
      requirements: input.requirements,
      records,
      current: {
        sourceRevision: input.sourceRevision,
        nodeFingerprints: input.currentFingerprints,
        generation: input.generation,
      },
      latestAttempts,
    })
    if (verdict.accepted) return acceptAfterRegression(input, records, attempts)
    const missing = input.requirements.find((requirement) => {
      const key = verificationRecordKey(requirement.nodeId, requirement.check.checkId)
      return !records.has(key)
    })
    if (missing !== undefined) {
      const key = verificationRecordKey(missing.nodeId, missing.check.checkId)
      return { kind: "blocked", reasons: [`missing verification record for ${key}`, ...verdict.reasons] }
    }
    const target = firstFailedFresh(input, records, latestAttempts)
    if (target === null) return { kind: "blocked", reasons: verdict.reasons }
    const signature = failureSignature(target.classification ?? "unknown", {
      exitCode: target.exitCode,
      signal: target.signal,
      timedOut: target.timedOut,
      output: target.outputSummary,
    })
    const decision = decideRepair(state, { record: target, signature, changedFiles: input.changedFiles })
    if (decision.kind === "blocked" || decision.kind === "exhausted") {
      return { kind: "blocked", reasons: [decision.reason, ...verdict.reasons] }
    }
    if (decision.kind === "renode") {
      if (input.retryNode === undefined || target.nodeId === undefined) {
        return { kind: "blocked", reasons: [`no worker retry path for ${target.checkId}`, ...verdict.reasons] }
      }
      await input.retryNode(target.nodeId)
    }
    if (input.isCancelled?.() === true) return { kind: "blocked", reasons: ["cancelled during repair"] }
    const result = await input.runner({
      check: decision.attempt.check,
      projectDir: input.projectDir,
      attempt: decision.attempt.attempt,
      isCancelled: input.isCancelled,
    })
    attempts += 1
    state = recordRepairAttempt(state, { record: target, signature, changedFiles: input.changedFiles })
    const fresh = withRecordClassification(
      buildVerificationRecord({
        runId: target.runId,
        parentSessionId: target.parentSessionId,
        rootSessionId: target.rootSessionId,
        check: decision.attempt.check,
        cwd: target.cwd,
        projectDir: target.projectDir,
        sourceRevision: input.sourceRevision,
        nodeFingerprint: target.nodeFingerprint,
        generation: input.generation,
        repairAttempt: decision.attempt.attempt,
        result,
      }),
      classifyCommandResult(result),
    )
    const keyed: DagVerificationRecord =
      target.nodeId === undefined ? fresh : { ...fresh, nodeId: target.nodeId, workerAttempt: target.workerAttempt }
    records.set(verificationRecordKey(target.nodeId, target.checkId), keyed)
    latestAttempts.set(verificationRecordKey(target.nodeId, target.checkId), decision.attempt.attempt)
    input.persistRecord?.(keyed)
  }
}

async function acceptAfterRegression(
  input: RepairDagRunInput,
  _records: ReadonlyMap<string, DagVerificationRecord>,
  attempts: number,
): Promise<DagRepairOutcome> {
  const reasons: string[] = []
  for (const check of input.regressionChecks) {
    if (input.isCancelled?.() === true) return { kind: "blocked", reasons: ["cancelled during repair"] }
    const result = await input.runner({ check, projectDir: input.projectDir, attempt: 0, isCancelled: input.isCancelled })
    if (result.timedOut || result.cancelled || !result.started || result.exitCode !== 0) {
      reasons.push(`regression check ${check.checkId} did not pass after repair`)
    }
  }
  if (reasons.length > 0) return { kind: "blocked", reasons }
  return { kind: "accepted", attempts }
}

function firstFailedFresh(
  input: RepairDagRunInput,
  records: ReadonlyMap<string, DagVerificationRecord>,
  latestAttempts: ReadonlyMap<string, number>,
): DagVerificationRecord | null {
  for (const requirement of input.requirements) {
    const record = records.get(verificationRecordKey(requirement.nodeId, requirement.check.checkId))
    if (record === undefined) continue
    const latest = latestAttempts.get(verificationRecordKey(requirement.nodeId, requirement.check.checkId)) ?? record.repairAttempt
    const freshness = checkVerificationFreshness(
      record,
      {
        sourceRevision: input.sourceRevision,
        nodeFingerprints: input.currentFingerprints,
        generation: input.generation,
      },
      latest,
    )
    if (!freshness.fresh) continue
    if (record.status === "passed") continue
    return record
  }
  return null
}
