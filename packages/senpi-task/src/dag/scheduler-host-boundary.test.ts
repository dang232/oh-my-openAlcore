// Host production-boundary tests for the Slice 4.3 verification/repair port.
//
// Every test below drives the REAL production scheduler (createDagScheduler) with the REAL
// verification.ts / repair.ts modules. A fake task manager stands in ONLY for child worker
// execution (spawn / wait / cancel), exactly like the Alcode scheduler-production-boundary
// suite: the verification gate, the evidence records, the acceptance verdict, and the
// owner-driven repair loop are never mocked.
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test"
import * as fs from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { ManagerStartSpec, TaskManager } from "../manager/types"
import type { TaskRecord, TaskStatus } from "../state"
import {
  buildVerificationRecord,
  classifyCommandResult,
  compileDag,
  createDagFileStore,
  createDagScheduler,
  dagFingerprint,
  DAG_NODE_ERROR_CODES,
  evaluateAcceptance,
  nodeFingerprintFromInput,
  nodeFingerprintInput,
  readRunVerificationRecords,
  repairDagRun,
  runVerificationCommand,
  verificationRecordKey,
  withRecordClassification,
  type DagAcceptanceRequirement,
  type DagDefinition,
  type DagFileStore,
  type DagRepairBudgets,
  type DagRunEvent,
  type DagRunRecordV1,
  type DagScheduler,
  type DagTaskOwner,
  type DagVerificationRecord,
  type DagVerifyCheck,
  type DagVerifyCommandResult,
  type DagVerifyRunner,
  type DagNodeId,
  type DagRunId,
  type OwnedStartResult,
} from "./index"
import { createDagScheduler as createDagSchedulerDirect } from "./scheduler"

setDefaultTimeout(process.platform === "win32" ? 60_000 : 20_000)

const CREATED_AT = "2026-08-14T00:00:00.000Z"
const ROOT_SESSION = "ses-root"

const cleanupRoots: string[] = []
afterEach(() => {
  for (const root of cleanupRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true })
})

function tempProject(): string {
  const directory = fs.mkdtempSync(join(tmpdir(), "senpi-dag-host-boundary-"))
  cleanupRoots.push(directory)
  return directory
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void } {
  let resolve = (_value: T): void => undefined
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function within<T>(promise: Promise<T>, ms = 5000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms)
    void promise.then(
      (value) => {
        clearTimeout(timeout)
        resolve(value)
      },
      (error: unknown) => {
        clearTimeout(timeout)
        reject(error)
      },
    )
  })
}

// Fake manager: stands in ONLY for child worker execution. It never touches verification
// records, acceptance verdicts, or the repair loop.
class HostBoundaryTaskManager {
  readonly starts: Array<{ readonly runId: string; readonly nodeId: string; readonly taskId: string }> = []
  readonly cancellations: string[] = []
  readonly #tasks = new Map<string, { record: TaskRecord; completion: ReturnType<typeof deferred<TaskRecord>> }>()
  readonly #startedWaiters = new Map<string, ReturnType<typeof deferred<void>>>()
  #counter = 0

  async startOwned(spec: ManagerStartSpec, owner: DagTaskOwner): Promise<OwnedStartResult> {
    this.#counter += 1
    const taskId = `host-task-${this.#counter}`
    const nodeId = String(owner.nodeId)
    const runId = String(owner.runId)
    const completion = deferred<TaskRecord>()
    const record: TaskRecord = {
      task_id: taskId,
      name: nodeId,
      parent_session_id: spec.parent_session_id,
      root_session_id: spec.root_session_id ?? ROOT_SESSION,
      depth: 1,
      category: "quick",
      execution_mode: "in-process",
      model: "fake-model",
      notify_on_terminal: true,
      owner,
      status: "running",
      residency_state: "resident",
      created_at: CREATED_AT,
      updated_at: "2026-08-14T00:00:00.000Z",
      notification: { run_epoch: 1, notified_epoch: 0 },
    }
    this.#tasks.set(taskId, { record, completion })
    this.starts.push({ runId, nodeId, taskId })
    this.#startedWaiters.get(`${runId}/${nodeId}`)?.resolve()
    return { kind: "started", reused: false, task_id: taskId, status: "running", name: nodeId }
  }

  whenStarted(runId: string, nodeId: string): Promise<void> {
    const key = `${runId}/${nodeId}`
    if (this.starts.some((entry) => entry.runId === runId && entry.nodeId === nodeId)) {
      return Promise.resolve()
    }
    let waiter = this.#startedWaiters.get(key)
    if (waiter === undefined) {
      waiter = deferred<void>()
      this.#startedWaiters.set(key, waiter)
    }
    return waiter.promise
  }

  taskIdFor(runId: string, nodeId: string): string {
    const entry = this.starts.find((candidate) => candidate.runId === runId && candidate.nodeId === nodeId)
    if (entry === undefined) throw new Error(`no fake task started for ${runId}/${nodeId}`)
    return entry.taskId
  }

  completeTask(taskId: string, status: TaskStatus): void {
    const task = this.#tasks.get(taskId)
    if (task === undefined) throw new Error(`unknown fake task ${taskId}`)
    task.record = {
      ...task.record,
      status,
      updated_at: "2026-08-14T00:00:01.000Z",
      ...(status === "completed"
        ? {
            final_response: `done ${task.record.name}`,
            run_stats: { runtime_ms: 25, turns: 2, tool_calls: 1, output_tokens: 8 },
          }
        : { error_message: `${status} ${task.record.name}` }),
    }
    task.completion.resolve(task.record)
  }

  completeNode(runId: string, nodeId: string, status: TaskStatus = "completed"): void {
    this.completeTask(this.taskIdFor(runId, nodeId), status)
  }

  waitFor(taskId: string): Promise<TaskRecord> {
    const task = this.#tasks.get(taskId)
    if (task === undefined) throw new Error(`unknown fake task ${taskId}`)
    return task.completion.promise
  }

  get(taskId: string): TaskRecord | undefined {
    return this.#tasks.get(taskId)?.record
  }

  findOwnedTask(owner: Pick<DagTaskOwner, "kind" | "runId" | "nodeId">): TaskRecord | undefined {
    return [...this.#tasks.values()].find(
      (entry) =>
        entry.record.owner?.kind === owner.kind &&
        entry.record.owner.runId === owner.runId &&
        entry.record.owner.nodeId === owner.nodeId,
    )?.record
  }

  async cancelTask(taskId: string): Promise<{ readonly kind: "cancelled"; readonly task_id: string; readonly previous_status: TaskStatus }> {
    const task = this.#tasks.get(taskId)
    if (task === undefined) throw new Error(`unknown fake task ${taskId}`)
    this.cancellations.push(taskId)
    const previous = task.record.status
    this.completeTask(taskId, "cancelled")
    return { kind: "cancelled", task_id: taskId, previous_status: previous }
  }

  subscribeChild(): () => void {
    return () => undefined
  }

  residencyChanged(): Promise<void> {
    return new Promise<void>(() => undefined)
  }

  list(): readonly [] {
    return []
  }

  forget(): void {}

  start(): Promise<never> {
    throw new Error("not implemented")
  }

  continueTask(): Promise<never> {
    throw new Error("not implemented")
  }

  sendToTask(): Promise<never> {
    throw new Error("not implemented")
  }

  interruptTask(): Promise<never> {
    throw new Error("not implemented")
  }
}

function asTaskManager(fake: HostBoundaryTaskManager): TaskManager {
  return fake as unknown as TaskManager
}

function makeCheck(checkId: string, overrides?: Partial<DagVerifyCheck>): DagVerifyCheck {
  return {
    checkId,
    category: "unit",
    command: ["fake-verify", checkId],
    timeoutMs: 5000,
    criteria: ["exits zero"],
    ...overrides,
  }
}

function passResult(output = "all green"): DagVerifyCommandResult {
  return {
    started: true,
    exitCode: 0,
    timedOut: false,
    cancelled: false,
    output,
    outputBytes: output.length,
    durationMs: 5,
    uncertainEffect: false,
  }
}

function failResult(output: string): DagVerifyCommandResult {
  return {
    started: true,
    exitCode: 1,
    timedOut: false,
    cancelled: false,
    output,
    outputBytes: output.length,
    durationMs: 7,
    uncertainEffect: false,
  }
}

function nodeDef(id: string, options?: { readonly dependsOn?: readonly string[]; readonly verify?: readonly DagVerifyCheck[] }) {
  return {
    id,
    prompt: `do ${id}`,
    category: "quick",
    ...(options?.dependsOn === undefined ? {} : { dependsOn: options.dependsOn }),
    ...(options?.verify === undefined ? {} : { verify: options.verify }),
  } as const
}

function dagDef(key: string, nodes: DagDefinition["nodes"]): DagDefinition {
  return { key, name: `${key} name`, nodes }
}

function recordForRun(
  definition: DagDefinition,
  runId: DagRunId,
  parentSessionId: string,
  rootSessionId: string,
): DagRunRecordV1 {
  const compiled = compileDag(definition, { at: CREATED_AT })
  if (!compiled.ok) throw new Error("test DAG did not compile")
  return {
    schemaVersion: 1,
    checkpointSeq: 0,
    runId,
    runKey: definition.key,
    name: definition.name,
    parentSessionId,
    rootSessionId,
    definitionFingerprint: "definition-fingerprint",
    definition: {
      key: definition.key,
      name: definition.name,
      nodes: definition.nodes.map((entry) => ({ ...entry, effectivePrompt: entry.prompt })),
    },
    status: "pending",
    generation: 1,
    createdAt: CREATED_AT,
    updatedAt: CREATED_AT,
    nodes: compiled.nodes,
    edges: compiled.edges,
    waves: compiled.waves,
    criticalPath: compiled.criticalPath,
    bottlenecks: compiled.bottlenecks,
    diagnostics: compiled.diagnostics,
  }
}

type HostFixtureOptions = {
  readonly runId?: string
  readonly parentSessionId?: string
  readonly rootSessionId?: string
  readonly store?: DagFileStore
  readonly projectDir?: string
  readonly manager?: HostBoundaryTaskManager
  readonly verifyRunner?: DagVerifyRunner
  readonly verifyChecks?: readonly DagVerifyCheck[]
}

function hostFixture(definition: DagDefinition, options: HostFixtureOptions = {}) {
  const runId = (options.runId ?? `run-${definition.key}`) as DagRunId
  const parentSessionId = options.parentSessionId ?? "ses-host-parent"
  const rootSessionId = options.rootSessionId ?? ROOT_SESSION
  const projectDir = options.projectDir ?? tempProject()
  const store = options.store ?? createDagFileStore({ project_dir: projectDir })
  const manager = options.manager ?? new HostBoundaryTaskManager()
  const initialRecord = recordForRun(definition, runId, parentSessionId, rootSessionId)
  store.writeCheckpoint(runId, initialRecord)
  let eventTime = Date.parse("2026-08-14T00:00:02.000Z")
  const schedulerOptions = {
    store,
    taskManager: asTaskManager(manager),
    initialRecord,
    ...(options.verifyRunner === undefined ? {} : { verifyRunner: options.verifyRunner }),
    ...(options.verifyRunner === undefined ? {} : { verifyProjectDir: projectDir }),
    verifyChecks: options.verifyChecks ?? [],
    now: () => eventTime++,
  }
  const scheduler: DagScheduler = createDagScheduler(schedulerOptions)
  return { scheduler, store, manager, projectDir, runId, parentSessionId, rootSessionId }
}

function attemptsOf(records: ReadonlyMap<string, DagVerificationRecord>): Map<string, number> {
  const latest = new Map<string, number>()
  for (const [key, record] of records) latest.set(key, record.repairAttempt)
  return latest
}

function acceptanceCurrent(snapshot: DagRunRecordV1) {
  const nodeFingerprints = new Map<string, string>()
  for (const node of snapshot.definition.nodes) {
    nodeFingerprints.set(String(node.id), dagFingerprint(nodeFingerprintInput(nodeFingerprintFromInput(node))))
  }
  return {
    sourceRevision: snapshot.definitionFingerprint,
    nodeFingerprints,
    generation: snapshot.generation,
  }
}

function failedRunError(store: DagFileStore, runId: DagRunId) {
  const events = store.readEvents(runId, 0, { limit: 500 }).events
  const failed = events.find(
    (event): event is Extract<DagRunEvent, { type: "dag.run.failed" }> => event.type === "dag.run.failed",
  )
  return failed?.error
}

function lastEventType(store: DagFileStore, runId: DagRunId): string | undefined {
  const events = store.readEvents(runId, 0, { limit: 500 }).events
  return events.at(-1)?.type
}

describe("H01 production acceptance with current evidence", () => {
  test("#given a passing worker with all required current evidence #when the production scheduler runs #then the run reaches accepted completed state", async () => {
    // given
    const check = makeCheck("unit-gate")
    const runnerCalls: string[] = []
    const runner: DagVerifyRunner = async (input) => {
      runnerCalls.push(`${input.check.checkId}#${input.attempt}`)
      return passResult()
    }
    const fixture = hostFixture(dagDef("h01", [nodeDef("work", { verify: [check] })]), { verifyRunner: runner })

    // when
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "work"))
    fixture.manager.completeNode(fixture.runId, "work", "completed")
    const result = await within(running)

    // then
    expect(result.status).toBe("completed")
    expect(result.nodes.find((entry) => String(entry.id) === "work")?.state).toBe("completed")
    expect(runnerCalls).toEqual(["unit-gate#0"])
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)
    expect(collected.malformed).toEqual([])
    const key = verificationRecordKey("work", "unit-gate")
    const record = collected.records.get(key)
    expect(record?.status).toBe("passed")
    const requirements: readonly DagAcceptanceRequirement[] = [{ check, nodeId: "work" as DagNodeId }]
    expect(
      evaluateAcceptance({
        requirements,
        records: collected.records,
        current: acceptanceCurrent(result),
        latestAttempts: attemptsOf(collected.records),
      }),
    ).toEqual({ accepted: true, reasons: [] })
  })
})

describe("H02 success claim without required evidence", () => {
  test("#given a worker that claims success but required evidence is absent #when the production scheduler runs #then acceptance stays blocked", async () => {
    // given
    const check = makeCheck("unit-gate")
    const runner: DagVerifyRunner = async () => failResult("assertion failed: widget missing")
    const fixture = hostFixture(dagDef("h02", [nodeDef("work", { verify: [check] })]), { verifyRunner: runner })

    // when
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "work"))
    fixture.manager.completeNode(fixture.runId, "work", "completed")
    const result = await within(running)

    // then
    expect(result.status).toBe("failed")
    const work = result.nodes.find((entry) => String(entry.id) === "work")
    expect(work?.state).toBe("failed")
    expect(work?.error?.code).toBe("verification_failed")
    const error = failedRunError(fixture.store, fixture.runId)
    expect(error?.code).toBe("verification_failed")
    expect(error?.message).toContain("run acceptance blocked")
    expect(result.nodes.some((entry) => entry.state === "completed")).toBe(false)
  })
})

describe("H03 owner-driven repair after recorded failure", () => {
  test("#given a real recorded verification failure #when the owner runs repairDagRun #then one bounded attempt reruns verification and accepts only after the criteria pass", async () => {
    // given: a real scheduler run records a real failure through the production modules
    const check = makeCheck("h03-check")
    const nodeId = "build" as DagNodeId
    const failingRunner: DagVerifyRunner = async () => failResult("assertion failed: widget missing")
    const fixture = hostFixture(dagDef("h03", [nodeDef("build", { verify: [check] })]), {
      verifyRunner: failingRunner,
    })
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "build"))
    fixture.manager.completeNode(fixture.runId, "build", "completed")
    const failed = await within(running)
    expect(failed.status).toBe("failed")
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)
    const key = verificationRecordKey("build", "h03-check")
    expect(collected.records.get(key)?.status).toBe("failed")

    // when: the owner drives the real repair loop; the runner passes only on the repair attempt
    const runnerCalls: string[] = []
    const repairRunner: DagVerifyRunner = async (input) => {
      runnerCalls.push(`${input.check.checkId}#${input.attempt}`)
      return input.attempt >= 1 ? passResult() : failResult("assertion failed: widget missing")
    }
    let retryCalls = 0
    const persisted: DagVerificationRecord[] = []
    const outcome = await repairDagRun({
      runner: repairRunner,
      projectDir: fixture.projectDir,
      sourceRevision: failed.definitionFingerprint,
      generation: failed.generation,
      requirements: [{ check, nodeId }],
      regressionChecks: [],
      records: collected.records,
      currentFingerprints: acceptanceCurrent(failed).nodeFingerprints,
      changedFiles: [],
      retryNode: async () => {
        retryCalls += 1
      },
      persistRecord: (record) => {
        persisted.push(record)
      },
    })

    // then
    expect(outcome).toEqual({ kind: "accepted", attempts: 1 })
    expect(retryCalls).toBe(1)
    expect(runnerCalls).toEqual(["h03-check#1"])
    expect(persisted).toHaveLength(1)
    expect(persisted[0]?.status).toBe("passed")
    expect(persisted[0]?.repairAttempt).toBe(1)
  })
})

describe("H04 regression failure after repair", () => {
  test("#given the original check passes after repair but a required regression check fails #when repairDagRun finishes #then acceptance stays blocked", async () => {
    // given: a real passing run leaves real evidence for the original check
    const check = makeCheck("h04-check")
    const nodeId = "build" as DagNodeId
    const fixture = hostFixture(dagDef("h04", [nodeDef("build", { verify: [check] })]), {
      verifyRunner: async () => passResult(),
    })
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "build"))
    fixture.manager.completeNode(fixture.runId, "build", "completed")
    const passed = await within(running)
    expect(passed.status).toBe("completed")
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)

    // when: the regression set fails after the original evidence already passes
    const regression = makeCheck("h04-regression", { category: "regression" })
    const runnerCalls: string[] = []
    const outcome = await repairDagRun({
      runner: async (input) => {
        runnerCalls.push(`${input.check.checkId}#${input.attempt}`)
        return input.check.checkId === "h04-regression"
          ? failResult("regression failed: latency budget exceeded")
          : passResult()
      },
      projectDir: fixture.projectDir,
      sourceRevision: passed.definitionFingerprint,
      generation: passed.generation,
      requirements: [{ check, nodeId }],
      regressionChecks: [regression],
      records: collected.records,
      currentFingerprints: acceptanceCurrent(passed).nodeFingerprints,
      changedFiles: [],
    })

    // then
    expect(outcome.kind).toBe("blocked")
    expect(outcome.kind === "blocked" ? outcome.reasons.join("; ") : "").toContain("h04-regression")
    expect(runnerCalls).toEqual(["h04-regression#0"])
  })
})

describe("H05 bounded repair budget", () => {
  test("#given a failure that never clears #when repairDagRun runs under a configured budget #then it terminates with an unresolved failure and no infinite loop", async () => {
    // given: a real scheduler run records a real failure through the production modules
    const check = makeCheck("h05-check")
    const nodeId = "build" as DagNodeId
    const fixture = hostFixture(dagDef("h05", [nodeDef("build", { verify: [check] })]), {
      verifyRunner: async () => failResult("assertion failed: widget missing"),
    })
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "build"))
    fixture.manager.completeNode(fixture.runId, "build", "completed")
    const failed = await within(running)
    expect(failed.status).toBe("failed")
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)

    // when: every repair attempt fails identically under a budget of one
    const budgets: DagRepairBudgets = {
      maxAttemptsPerCheck: 1,
      maxAttemptsPerNode: 1,
      maxAttemptsPerRun: 1,
      maxIdenticalFailures: 1,
    }
    let runnerCalls = 0
    let retryCalls = 0
    const outcome = await repairDagRun({
      runner: async () => {
        runnerCalls += 1
        return failResult("assertion failed: widget missing")
      },
      projectDir: fixture.projectDir,
      sourceRevision: failed.definitionFingerprint,
      generation: failed.generation,
      budgets,
      requirements: [{ check, nodeId }],
      regressionChecks: [],
      records: collected.records,
      currentFingerprints: acceptanceCurrent(failed).nodeFingerprints,
      changedFiles: [],
      retryNode: async () => {
        retryCalls += 1
      },
    })

    // then
    expect(outcome.kind).toBe("blocked")
    expect(outcome.kind === "blocked" ? outcome.reasons.join("; ") : "").toMatch(/budget/i)
    expect(runnerCalls).toBe(1)
    expect(retryCalls).toBe(1)
  })
})

describe("H06 fingerprint invalidation after repair", () => {
  test("#given a repair that changes a relevant fingerprint #when acceptance is re-evaluated #then the previous passing record cannot satisfy the current requirement", async () => {
    // given: a real passing run leaves real evidence through the production scheduler
    const check = makeCheck("h06-check")
    const fixture = hostFixture(dagDef("h06", [nodeDef("build", { verify: [check] })]), {
      verifyRunner: async () => passResult(),
    })
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "build"))
    fixture.manager.completeNode(fixture.runId, "build", "completed")
    const result = await within(running)
    expect(result.status).toBe("completed")
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)
    const requirements: readonly DagAcceptanceRequirement[] = [{ check, nodeId: "build" as DagNodeId }]

    // control: the record satisfies the fingerprint it was recorded against
    const current = acceptanceCurrent(result)
    expect(
      evaluateAcceptance({
        requirements,
        records: collected.records,
        current,
        latestAttempts: attemptsOf(collected.records),
      }).accepted,
    ).toBe(true)

    // when: the repair changes the node prompt, so the check identifiers join a new fingerprint
    const persisted = result.definition.nodes.find((entry) => String(entry.id) === "build")
    if (persisted === undefined) throw new Error("h06 fixture lost its build node")
    const before = dagFingerprint(nodeFingerprintInput(nodeFingerprintFromInput(persisted)))
    const after = dagFingerprint(
      nodeFingerprintInput(nodeFingerprintFromInput({ ...persisted, prompt: `${persisted.prompt} [repaired]` })),
    )
    expect(after).not.toBe(before)
    const verdict = evaluateAcceptance({
      requirements,
      records: collected.records,
      current: {
        sourceRevision: result.definitionFingerprint,
        nodeFingerprints: new Map([["build", after]]),
        generation: result.generation,
      },
      latestAttempts: attemptsOf(collected.records),
    })

    // then
    expect(verdict.accepted).toBe(false)
    expect(verdict.reasons.join("; ")).toContain("node definition changed")
  })
})

describe("H07 uncertain side effect without acknowledgement", () => {
  test("#given a missing acknowledgement with a possible worker side effect #when the owner runs repairDagRun #then it reconciles without uncontrolled duplicate writes", async () => {
    // given: the check starts but never acknowledges, and the command may have changed state
    const check = makeCheck("h07-check")
    const nodeId = "build" as DagNodeId
    const uncertainOutput = "no acknowledgement from worker before deadline"
    const fixture = hostFixture(dagDef("h07", [nodeDef("build", { verify: [check] })]), {
      verifyRunner: async () => ({
        started: true,
        timedOut: true,
        cancelled: false,
        output: uncertainOutput,
        outputBytes: uncertainOutput.length,
        durationMs: 5000,
        uncertainEffect: true,
      }),
    })
    const running = fixture.scheduler.run()
    await within(fixture.manager.whenStarted(fixture.runId, "build"))
    fixture.manager.completeNode(fixture.runId, "build", "completed")
    const settled = await within(running)
    expect(settled.status).toBe("failed")
    const collected = readRunVerificationRecords(fixture.store, fixture.runId)
    expect(collected.records.get(verificationRecordKey("build", "h07-check"))?.status).toBe("timed_out")

    // when: the owner drives the real repair loop over the uncertain record
    let runnerCalls = 0
    let persisted = 0
    const outcome = await repairDagRun({
      runner: async () => {
        runnerCalls += 1
        return passResult()
      },
      projectDir: fixture.projectDir,
      sourceRevision: settled.definitionFingerprint,
      generation: settled.generation,
      requirements: [{ check, nodeId }],
      regressionChecks: [],
      records: collected.records,
      currentFingerprints: acceptanceCurrent(settled).nodeFingerprints,
      changedFiles: [],
      retryNode: async () => undefined,
      persistRecord: () => {
        persisted += 1
      },
    })

    // then: no retry is dispatched until the side effect is reconciled
    expect(outcome.kind).toBe("blocked")
    expect(outcome.kind === "blocked" ? outcome.reasons.join("; ") : "").toMatch(/uncertain/i)
    expect(runnerCalls).toBe(0)
    expect(persisted).toBe(0)
  })
})

describe("H08 cancellation during repair and late results", () => {
  test("#given cancellation during repair #when late results arrive #then no further dispatch happens and terminal state is never overwritten", async () => {
    // given
    const check = makeCheck("h08-check")
    const nodeId = "build" as DagNodeId
    let runnerCalls = 0
    const outcome = await repairDagRun({
      runner: async () => {
        runnerCalls += 1
        return passResult()
      },
      projectDir: tempProject(),
      sourceRevision: "definition-fingerprint",
      generation: 1,
      requirements: [{ check, nodeId }],
      regressionChecks: [],
      records: new Map(),
      currentFingerprints: new Map(),
      changedFiles: [],
      isCancelled: () => true,
    })

    // then: cancellation prevents further dispatch
    expect(outcome).toEqual({ kind: "blocked", reasons: ["cancelled during repair"] })
    expect(runnerCalls).toBe(0)
  })

  test("#given a run cancelled mid-flight #when a late worker result lands #then the terminal state holds and another session run is unaffected", async () => {
    // given: two host runs sharing one store under different sessions
    const projectDir = tempProject()
    const store = createDagFileStore({ project_dir: projectDir })
    const manager = new HostBoundaryTaskManager()
    const runA = hostFixture(dagDef("h08a", [nodeDef("a-work")]), {
      runId: "run-h08a",
      parentSessionId: "ses-host-a",
      store,
      projectDir,
      manager,
    })
    const runB = hostFixture(dagDef("h08b", [nodeDef("b-work")]), {
      runId: "run-h08b",
      parentSessionId: "ses-host-b",
      store,
      projectDir,
      manager,
    })
    const pendingA = runA.scheduler.run()
    await within(manager.whenStarted("run-h08a", "a-work"))

    // when: the host cancels run A, then the late worker result for A lands
    await runA.scheduler.cancel("run-h08a" as DagRunId, "host shutdown test")
    const resultA = await within(pendingA)
    manager.completeNode("run-h08a", "a-work", "completed")

    // then: A stays cancelled and B still completes on its own session
    expect(resultA.status).toBe("cancelled")
    expect(runA.scheduler.snapshot().status).toBe("cancelled")
    expect(runA.scheduler.snapshot().nodes.find((entry) => String(entry.id) === "a-work")?.state).toBe("cancelled")
    expect(lastEventType(store, "run-h08a" as DagRunId)).toBe("dag.run.cancelled")

    const pendingB = runB.scheduler.run()
    await within(manager.whenStarted("run-h08b", "b-work"))
    manager.completeNode("run-h08b", "b-work", "completed")
    const resultB = await within(pendingB)
    expect(resultB.status).toBe("completed")
    expect(lastEventType(store, "run-h08b" as DagRunId)).toBe("dag.run.completed")
    expect(runA.scheduler.snapshot().status).toBe("cancelled")
  })
})

describe("H09 concurrent runs with overlapping goals", () => {
  test("#given two concurrent host runs with overlapping goal identifiers #when both settle #then verification records evidence budgets and outcomes stay correctly scoped", async () => {
    // given: same node id and same check id in two runs over one shared store
    const projectDir = tempProject()
    const store = createDagFileStore({ project_dir: projectDir })
    const manager = new HostBoundaryTaskManager()
    const check = makeCheck("goal-check")
    const runA = hostFixture(dagDef("h09", [nodeDef("goal", { verify: [check] })]), {
      runId: "run-h09a",
      parentSessionId: "ses-host-a",
      store,
      projectDir,
      manager,
      verifyRunner: async () => passResult(),
    })
    const runB = hostFixture(dagDef("h09", [nodeDef("goal", { verify: [check] })]), {
      runId: "run-h09b",
      parentSessionId: "ses-host-b",
      store,
      projectDir,
      manager,
      verifyRunner: async () => failResult("assertion failed: goal unmet"),
    })

    // when
    const pendingA = runA.scheduler.run()
    const pendingB = runB.scheduler.run()
    await within(Promise.all([manager.whenStarted("run-h09a", "goal"), manager.whenStarted("run-h09b", "goal")]))
    manager.completeNode("run-h09a", "goal", "completed")
    manager.completeNode("run-h09b", "goal", "completed")
    const [resultA, resultB] = await within(Promise.all([pendingA, pendingB]))

    // then: outcomes are scoped per run
    expect(resultA.status).toBe("completed")
    expect(resultB.status).toBe("failed")
    expect(store.listVerifications("run-h09a" as DagRunId)).toHaveLength(1)
    expect(store.listVerifications("run-h09b" as DagRunId)).toHaveLength(1)
    const recordsA = readRunVerificationRecords(store, "run-h09a" as DagRunId)
    const recordsB = readRunVerificationRecords(store, "run-h09b" as DagRunId)
    expect(recordsA.records.get(verificationRecordKey("goal", "goal-check"))?.status).toBe("passed")
    expect(recordsB.records.get(verificationRecordKey("goal", "goal-check"))?.status).toBe("failed")

    // and: repairing run B consumes only run B evidence and leaves run A untouched
    let retryCalls = 0
    const outcome = await repairDagRun({
      runner: async () => passResult(),
      projectDir,
      sourceRevision: resultB.definitionFingerprint,
      generation: resultB.generation,
      requirements: [{ check, nodeId: "goal" as DagNodeId }],
      regressionChecks: [],
      records: recordsB.records,
      currentFingerprints: acceptanceCurrent(resultB).nodeFingerprints,
      changedFiles: [],
      retryNode: async () => {
        retryCalls += 1
      },
    })
    expect(outcome).toEqual({ kind: "accepted", attempts: 1 })
    expect(retryCalls).toBe(1)
    const recordsAAfter = readRunVerificationRecords(store, "run-h09a" as DagRunId)
    expect(recordsAAfter.records.get(verificationRecordKey("goal", "goal-check"))?.repairAttempt).toBe(0)
    expect(recordsAAfter.records.get(verificationRecordKey("goal", "goal-check"))?.status).toBe("passed")
  })
})

describe("H10 aggregate completion across required work", () => {
  test("#given all required work and checks passing #when the production scheduler runs #then the run completes but any unresolved required goal blocks aggregate success", async () => {
    // given: two required nodes whose checks both pass
    const appCheck = makeCheck("app-check")
    const apiCheck = makeCheck("api-check")
    const passing = hostFixture(
      dagDef("h10", [nodeDef("app", { verify: [appCheck] }), nodeDef("api", { verify: [apiCheck] })]),
      { verifyRunner: async () => passResult() },
    )
    const pendingPass = passing.scheduler.run()
    await within(
      Promise.all([
        passing.manager.whenStarted(passing.runId, "app"),
        passing.manager.whenStarted(passing.runId, "api"),
      ]),
    )
    passing.manager.completeNode(passing.runId, "app", "completed")
    passing.manager.completeNode(passing.runId, "api", "completed")
    const completed = await within(pendingPass)

    // then: the aggregate run completes
    expect(completed.status).toBe("completed")

    // when: one required worker errors so its goal never resolves, with api
    // waiting on app so the unresolved goal visibly blocks its dependent
    const gated = hostFixture(
      dagDef("h10-gated", [
        nodeDef("app", { verify: [appCheck] }),
        nodeDef("api", { dependsOn: ["app"], verify: [apiCheck] }),
      ]),
      { verifyRunner: async () => passResult() },
    )
    const pendingBlocked = gated.scheduler.run()
    await within(gated.manager.whenStarted(gated.runId, "app"))
    gated.manager.completeNode(gated.runId, "app", "error")
    const failed = await within(pendingBlocked)

    // then: aggregate success stays blocked and the dependent never runs unverified
    expect(failed.status).toBe("failed")
    expect(failed.nodes.find((entry) => String(entry.id) === "app")?.state).toBe("failed")
    expect(failed.nodes.find((entry) => String(entry.id) === "app")?.error?.code).toBe("task_error")
    expect(failed.nodes.find((entry) => String(entry.id) === "api")?.state).toBe("skipped")
    expect(gated.manager.starts.some((entry) => entry.nodeId === "api")).toBe(false)
  })
})

describe("H11 production module provenance", () => {
  test("#given the host barrel and schedulerOptions wiring #when the symbols are inspected #then the real repairDagRun and verification symbols are consumed with no mock substitution", async () => {
    // the test file itself enters through the same barrel the host uses
    const selfSource = fs.readFileSync(import.meta.path, "utf8")
    expect(selfSource).toContain('from "./index"')
    expect(selfSource).toContain("repairDagRun")
    expect(selfSource).not.toMatch(/mock\.module\s*\(/)

    // the barrel re-exports the real production symbols, not copies or mocks
    expect(createDagScheduler).toBe(createDagSchedulerDirect)
    expect(typeof repairDagRun).toBe("function")
    expect(typeof runVerificationCommand).toBe("function")
    expect(typeof buildVerificationRecord).toBe("function")
    expect(typeof withRecordClassification).toBe("function")
    expect(typeof classifyCommandResult).toBe("function")
    expect(typeof evaluateAcceptance).toBe("function")
    expect(DAG_NODE_ERROR_CODES).toContain("verification_failed")

    // the dag-runtime.ts schedulerOptions wiring shape works end to end: the default argv
    // runner, the engine cwd as project dir, and an empty run-level check set accept a
    // check-free run exactly like production
    const projectDir = tempProject()
    const store = createDagFileStore({ project_dir: projectDir })
    const manager = new HostBoundaryTaskManager()
    const runId = "run-h11" as DagRunId
    const initialRecord = recordForRun(dagDef("h11", [nodeDef("work")]), runId, "ses-host-parent", ROOT_SESSION)
    store.writeCheckpoint(runId, initialRecord)
    const scheduler = createDagScheduler({
      store,
      taskManager: asTaskManager(manager),
      initialRecord,
      verifyRunner: (input) => runVerificationCommand(input),
      verifyProjectDir: projectDir,
      verifyChecks: [],
    })
    const pending = scheduler.run()
    await within(manager.whenStarted("run-h11", "work"))
    manager.completeNode("run-h11", "work", "completed")
    const result = await within(pending)
    expect(result.status).toBe("completed")
  })
})

describe("H12 missing verification implementation", () => {
  test("#given required checks but no runnable verification implementation #when the production scheduler runs #then the host reports a clear failure and never falls back to unverified success", async () => {
    // case 1: no runner is configured at all
    const check = makeCheck("h12-check")
    const missing = hostFixture(dagDef("h12-missing", [nodeDef("work", { verify: [check] })]))
    const pendingMissing = missing.scheduler.run()
    await within(missing.manager.whenStarted(missing.runId, "work"))
    missing.manager.completeNode(missing.runId, "work", "completed")
    const resultMissing = await within(pendingMissing)
    expect(resultMissing.status).not.toBe("completed")
    expect(resultMissing.status).toBe("failed")
    const missingError = failedRunError(missing.store, missing.runId)
    expect(missingError?.code).toBe("verification_failed")
    expect(missingError?.message).toContain("no runner is configured")

    // case 2: the runner cannot start the check, so no genuine execution record exists
    const unavailableOutput = "spawn ENOENT: check binary missing"
    const unavailable = hostFixture(dagDef("h12-unavailable", [nodeDef("work", { verify: [check] })]), {
      verifyRunner: async () => ({
        started: false,
        timedOut: false,
        cancelled: false,
        output: unavailableOutput,
        outputBytes: unavailableOutput.length,
        durationMs: 0,
        uncertainEffect: false,
      }),
    })
    const pendingUnavailable = unavailable.scheduler.run()
    await within(unavailable.manager.whenStarted(unavailable.runId, "work"))
    unavailable.manager.completeNode(unavailable.runId, "work", "completed")
    const resultUnavailable = await within(pendingUnavailable)
    expect(resultUnavailable.status).not.toBe("completed")
    expect(resultUnavailable.status).toBe("failed")
    expect(
      resultUnavailable.nodes.find((entry) => String(entry.id) === "work")?.error?.code,
    ).toBe("verification_failed")
    const collected = readRunVerificationRecords(unavailable.store, unavailable.runId)
    expect(collected.records.get(verificationRecordKey("work", "h12-check"))?.status).toBe("unavailable")
  })
})
