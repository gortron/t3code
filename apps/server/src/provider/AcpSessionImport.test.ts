import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, describe, it } from "@effect/vitest";
import {
  AcpRegistryOperationError,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type AcpRegistrySession,
  type ServerProvider,
} from "@t3tools/contracts";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import { TestClock } from "effect/testing";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AcpSessionImport from "./AcpSessionImport.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderRegistryMock from "./testUtils/providerRegistryMock.ts";

const driver = ProviderDriverKind.make("acpRegistry");
const instanceId = ProviderInstanceId.make("acpRegistry_devin");
const projectId = ProjectId.make("project:acp-import");
const projectRoot = "/work/acp-import";
const pollInterval = "5 minutes";

type ScanResult = Effect.Effect<
  Option.Option<ReadonlyArray<AcpRegistrySession>>,
  AcpRegistryOperationError
>;

const session = (sessionId: string, cwd = projectRoot): AcpRegistrySession => ({
  sessionId,
  cwd,
  additionalDirectories: [],
  title: `Session ${sessionId}`,
  updatedAt: "2026-10-01T00:00:00.000Z",
  importedThreadId: null,
});

const makeHarness = Effect.gen(function* () {
  const listed = yield* Ref.make<ReadonlyArray<AcpRegistrySession>>([]);
  const nextScan = yield* Ref.make<ScanResult | undefined>(undefined);
  const scanCount = yield* Ref.make(0);
  const scans = yield* Queue.unbounded<ReadonlyArray<string>>();
  const opportunistic = yield* Ref.make(true);
  const instances = yield* Ref.make<ReadonlyArray<ProviderInstance>>([]);

  const makeInstance = (options: {
    readonly autoImportSessions?: boolean;
    readonly nativeSessions?: ServerProvider["nativeSessions"];
  }): ProviderInstance =>
    ({
      instanceId,
      driverKind: driver,
      enabled: true,
      snapshot: {
        getSnapshot: Effect.succeed({
          nativeSessions:
            "nativeSessions" in options
              ? options.nativeSessions
              : { canList: true, canLoad: true, canResume: false },
        } as ServerProvider),
      },
      acpSessionManagement: {
        autoImportSessions: options.autoImportSessions ?? true,
        listSessions: () =>
          Ref.get(listed).pipe(
            Effect.map((sessions) => ({
              sessions,
              nextCursor: null,
              canLoad: true,
              canResume: false,
              canDelete: false,
            })),
          ),
        scanSessions: (cwds: ReadonlyArray<string>) =>
          Effect.gen(function* () {
            yield* Ref.update(scanCount, (count) => count + 1);
            yield* Queue.offer(scans, cwds);
            const override = yield* Ref.getAndSet(nextScan, undefined);
            return yield* override ?? Effect.asSome(Ref.get(listed));
          }),
      },
    }) as unknown as ProviderInstance;

  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const launch: ThreadLaunchService.ThreadLaunchService["Service"]["launch"] = (input) =>
    Effect.gen(function* () {
      const threadId = input.threadId!;
      const now = yield* DateTime.now;
      const thread = {
        createdBy: input.createdBy,
        creationSource: input.creationSource,
        id: threadId,
        projectId: input.projectId,
        title: input.title,
        providerInstanceId: input.modelSelection.instanceId,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      yield* projections.apply({
        id: EventId.make(`event:${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      });
      const imported = input.importedNativeThread!;
      yield* bindNativeSession({
        threadId,
        providerThreadId: IdAllocator.deriveProviderThread({
          driver,
          providerInstanceId: instanceId,
          nativeThreadId: imported.ref.nativeId,
        }),
        nativeId: imported.ref.nativeId,
      });
      return {
        threadId,
        projection: yield* projections.getThreadProjection(threadId),
        resumed: false,
      };
    }).pipe(Effect.orDie);

  const bindNativeSession = (input: {
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
    readonly nativeId: string;
  }) =>
    DateTime.now.pipe(
      Effect.flatMap((now) =>
        projections.apply({
          id: EventId.make(`event:${input.providerThreadId}:updated`),
          type: "provider-thread.updated",
          threadId: input.threadId,
          driver,
          occurredAt: now,
          payload: {
            id: input.providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: input.threadId,
            ownerNodeId: null,
            nativeThreadRef: { driver, nativeId: input.nativeId, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "not_loaded",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        }),
      ),
    );

  const project = {
    id: projectId,
    title: "ACP import",
    workspaceRoot: projectRoot,
    repositoryIdentity: null,
    faviconPath: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  } as const;

  const service = yield* AcpSessionImport.AcpSessionImport.pipe(
    Effect.provide(
      AcpSessionImport.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
            Layer.mock(ProjectService.ProjectService)({
              getById: (id) =>
                Effect.succeed(id === projectId ? Option.some(project) : Option.none()),
              listShells: () =>
                Effect.succeed([{ id: projectId, workspaceRoot: projectRoot }] as never),
            }),
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              listInstances: Ref.get(instances),
              getInstance: (id) =>
                Ref.get(instances).pipe(
                  Effect.map((current) => current.find((instance) => instance.instanceId === id)),
                ),
            }),
            ProviderRegistryMock.layer(),
            Layer.mock(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({
              withSessionMutation: (effect) => effect,
            }),
            Layer.mock(ThreadLaunchService.ThreadLaunchService)({ launch }),
            Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
              enqueueCommand: (effect) => effect,
            }),
            ServerSettings.layerTest(),
            Layer.mock(BackgroundPolicy.BackgroundPolicy)({
              shouldRunOpportunisticWork: Ref.get(opportunistic),
            }),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  );

  const setInstance = (instance: ProviderInstance) => Ref.set(instances, [instance]);
  const threads = projections
    .getShellSnapshot()
    .pipe(Effect.map((snapshot) => snapshot.threads.map((thread) => thread.id)));

  return {
    service,
    projections,
    listed,
    nextScan,
    scanCount,
    scans,
    opportunistic,
    makeInstance,
    setInstance,
    bindNativeSession,
    threads,
  };
}).pipe(Effect.provide(ProjectionStore.layerMemory));

const importedThread = (sessionId: string) =>
  IdAllocator.deriveThreadFromProviderThread({
    driver,
    providerInstanceId: instanceId,
    nativeThreadId: sessionId,
  });

const scanFailure = Effect.fail(
  new AcpRegistryOperationError({ reason: "probe_failed", message: "agent crashed" }),
);

describe("AcpSessionImport", () => {
  it.effect("imports an external session into its project with server provenance", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      yield* Ref.set(harness.listed, [session("external-1")]);

      yield* harness.service.sweep;

      const threadId = importedThread("external-1");
      assert.deepStrictEqual(yield* harness.threads, [threadId]);
      const thread = yield* harness.projections.getThread(threadId);
      assert.strictEqual(thread.projectId, projectId);
      assert.strictEqual(thread.title, "Session external-1");
      assert.strictEqual(thread.createdBy, "system");
      assert.strictEqual(thread.creationSource, "server");
      assert.deepStrictEqual(yield* Queue.takeAll(harness.scans), [[projectRoot]]);
    }),
  );

  it.effect.each([
    ["without session listing", { canList: false, canLoad: true, canResume: true }],
    ["that has not been probed", undefined],
  ] as const)("does not scan an instance %s", ([, nativeSessions]) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({ nativeSessions }));
      yield* Ref.set(harness.listed, [session("external-1")]);

      yield* harness.service.sweep;

      assert.strictEqual(yield* Ref.get(harness.scanCount), 0);
      assert.deepStrictEqual(yield* harness.threads, []);
    }),
  );

  it.effect("does not scan an instance with auto-import turned off", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({ autoImportSessions: false }));
      yield* Ref.set(harness.listed, [session("external-1")]);

      yield* harness.service.sweep;

      assert.strictEqual(yield* Ref.get(harness.scanCount), 0);
      assert.deepStrictEqual(yield* harness.threads, []);
    }),
  );

  it.effect("does not scan while no client wants opportunistic background work", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      yield* Ref.set(harness.opportunistic, false);

      yield* harness.service.sweep;

      assert.strictEqual(yield* Ref.get(harness.scanCount), 0);
    }),
  );

  it.effect("never duplicates sessions T3 already knows", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      yield* Ref.set(harness.listed, [session("previously-imported")]);
      yield* harness.service.sweep;
      const [previouslyImported] = yield* harness.threads;

      const t3Created = yield* createThread(harness, "thread:t3-created");
      yield* harness.bindNativeSession({
        threadId: t3Created,
        providerThreadId: ProviderThreadId.make("provider-thread:t3-created"),
        nativeId: "t3-created",
      });
      const deleted = yield* createThread(harness, "thread:deleted");
      yield* harness.bindNativeSession({
        threadId: deleted,
        providerThreadId: ProviderThreadId.make("provider-thread:deleted"),
        nativeId: "deleted",
      });
      yield* deleteThread(harness, deleted);
      yield* Ref.set(harness.listed, [
        session("previously-imported"),
        session("t3-created"),
        session("deleted"),
      ]);

      yield* harness.service.sweep;
      yield* harness.service.sweep;

      assert.deepStrictEqual(
        (yield* harness.threads).toSorted(),
        [previouslyImported!, t3Created].toSorted(),
      );
    }),
  );

  it.effect("skips sessions outside every project root", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      yield* Ref.set(harness.listed, [
        session("subdirectory", `${projectRoot}/packages/app`),
        session("elsewhere", "/work/other"),
        session("root", `${projectRoot}/`),
      ]);

      yield* harness.service.sweep;

      assert.deepStrictEqual(yield* harness.threads, [importedThread("root")]);
    }),
  );

  it.effect("keeps an imported thread when its session disappears from the agent", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      yield* Ref.set(harness.listed, [session("vanishing")]);
      yield* harness.service.sweep;

      yield* Ref.set(harness.listed, []);
      yield* harness.service.sweep;

      assert.deepStrictEqual(yield* harness.threads, [importedThread("vanishing")]);
    }),
  );

  it.effect("backs off an instance whose scans keep failing", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      const failingSweep = Ref.set(harness.nextScan, scanFailure).pipe(
        Effect.andThen(harness.service.sweep),
      );

      yield* failingSweep;
      yield* TestClock.adjust(pollInterval);
      yield* failingSweep;
      assert.strictEqual(yield* Ref.get(harness.scanCount), 2);

      yield* TestClock.adjust(pollInterval);
      yield* harness.service.sweep;
      assert.strictEqual(yield* Ref.get(harness.scanCount), 2);

      yield* TestClock.adjust(pollInterval);
      yield* Ref.set(harness.listed, [session("after-recovery")]);
      yield* harness.service.sweep;
      assert.strictEqual(yield* Ref.get(harness.scanCount), 3);
      assert.deepStrictEqual(yield* harness.threads, [importedThread("after-recovery")]);

      yield* harness.service.sweep;
      assert.strictEqual(yield* Ref.get(harness.scanCount), 4);
    }),
  );

  it.effect("sweeps once at start and again after each background refresh interval", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));

      yield* harness.service.start();
      yield* Queue.take(harness.scans);
      yield* TestClock.adjust(pollInterval);
      yield* Queue.take(harness.scans);

      assert.strictEqual(yield* Ref.get(harness.scanCount), 2);
    }).pipe(Effect.scoped),
  );

  it.effect("lists a T3-created session as already in T3 and refuses to import it again", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* harness.setInstance(harness.makeInstance({}));
      const t3Created = yield* createThread(harness, "thread:t3-created");
      yield* harness.bindNativeSession({
        threadId: t3Created,
        providerThreadId: ProviderThreadId.make("provider-thread:t3-created"),
        nativeId: "t3-created",
      });
      yield* Ref.set(harness.listed, [session("t3-created"), session("external")]);

      const listed = yield* harness.service.listSessions({ instanceId, projectId });
      const imported = yield* harness.service.importSession({
        instanceId,
        projectId,
        sessionId: "t3-created",
      });

      assert.deepStrictEqual(
        listed.sessions.map((entry) => [entry.sessionId, entry.importedThreadId]),
        [
          ["t3-created", t3Created],
          ["external", null],
        ],
      );
      assert.deepStrictEqual(imported, { threadId: t3Created, imported: false });
      assert.deepStrictEqual(yield* harness.threads, [t3Created]);
    }),
  );
});

type Harness = Effect.Success<typeof makeHarness>;

const createThread = (harness: Harness, id: string) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make(id);
    const now = yield* DateTime.now;
    yield* harness.projections.apply({
      id: EventId.make(`event:${id}:created`),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId,
        title: id,
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "default" },
        runtimeMode: "approval-required",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    });
    return threadId;
  });

const deleteThread = (harness: Harness, threadId: ThreadId) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const thread = yield* harness.projections.getThread(threadId);
    yield* harness.projections.apply({
      id: EventId.make(`event:${threadId}:deleted`),
      type: "thread.deleted",
      threadId,
      occurredAt: now,
      payload: { ...thread, deletedAt: now },
    });
  });
