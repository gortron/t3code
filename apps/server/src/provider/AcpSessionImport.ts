import {
  AcpRegistryOperationError,
  CommandId,
  DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL,
  type AcpRegistryDeleteSessionInput,
  type AcpRegistryDeleteSessionResult,
  type AcpRegistryImportSessionInput,
  type AcpRegistryImportSessionResult,
  type AcpRegistryListSessionsInput,
  type AcpRegistryListSessionsResult,
  type AcpRegistrySession,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type Project,
  type ProjectId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import { resolveServerBackgroundActivitySettings } from "@t3tools/shared/backgroundActivitySettings";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import type * as Scope from "effect/Scope";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";

type AcpSessionManagement = NonNullable<ProviderInstance["acpSessionManagement"]>;

export class AcpSessionImport extends Context.Service<
  AcpSessionImport,
  {
    readonly resolveProject: (
      projectId: ProjectId,
    ) => Effect.Effect<Project, AcpRegistryOperationError>;
    readonly resolveSessionManagement: (
      instanceId: ProviderInstanceId,
    ) => Effect.Effect<
      { readonly instance: ProviderInstance; readonly manager: AcpSessionManagement },
      AcpRegistryOperationError
    >;
    readonly listSessions: (
      input: AcpRegistryListSessionsInput,
    ) => Effect.Effect<AcpRegistryListSessionsResult, AcpRegistryOperationError>;
    readonly importSession: (
      input: AcpRegistryImportSessionInput,
    ) => Effect.Effect<AcpRegistryImportSessionResult, AcpRegistryOperationError>;
    readonly deleteSession: (
      input: AcpRegistryDeleteSessionInput,
    ) => Effect.Effect<AcpRegistryDeleteSessionResult, AcpRegistryOperationError>;
    /** Imports the native sessions T3 does not know yet from every opted-in instance. */
    readonly sweep: Effect.Effect<void>;
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/provider/AcpSessionImport") {}

interface PollState {
  readonly failures: number;
  readonly nextAttemptAt: number;
}

const MAX_BACKOFF_EXPONENT = 4;

const make = Effect.gen(function* () {
  const projectService = yield* ProjectService.ProjectService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const providerInstances = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const runtimeCoordinator = yield* AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator;
  const threadLaunch = yield* ThreadLaunchService.ThreadLaunchService;
  const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const backgroundPolicy = yield* BackgroundPolicy.BackgroundPolicy;
  const crypto = yield* Crypto.Crypto;
  // Keyed by instance object: a settings change rebuilds the instance, which
  // resets its backoff.
  const pollStates = new WeakMap<ProviderInstance, PollState>();

  const resolveProject = Effect.fn("AcpSessionImport.resolveProject")(function* (
    projectId: ProjectId,
  ) {
    const project = yield* projectService.getById(projectId).pipe(
      Effect.mapError(
        (cause) =>
          new AcpRegistryOperationError({
            reason: "project_not_found",
            message: `Project ${projectId} is unavailable.`,
            cause,
          }),
      ),
    );
    return yield* Option.match(project, {
      onNone: () =>
        Effect.fail(
          new AcpRegistryOperationError({
            reason: "project_not_found",
            message: `Project ${projectId} was not found.`,
          }),
        ),
      onSome: Effect.succeed,
    });
  });

  const resolveSessionManagement = Effect.fn("AcpSessionImport.resolveSessionManagement")(
    function* (instanceId: ProviderInstanceId) {
      const instance = yield* providerInstances.getInstance(instanceId);
      if (instance === undefined) {
        return yield* new AcpRegistryOperationError({
          reason: "instance_not_found",
          message: `Provider instance ${instanceId} was not found.`,
        });
      }
      if (instance.acpSessionManagement === undefined) {
        return yield* new AcpRegistryOperationError({
          reason: "session_list_unsupported",
          message: `Provider instance ${instanceId} does not expose ACP session management.`,
        });
      }
      return { instance, manager: instance.acpSessionManagement };
    },
  );

  const importedThreadId = (instance: ProviderInstance, sessionId: string) =>
    IdAllocator.deriveThreadFromProviderThread({
      driver: instance.driverKind,
      providerInstanceId: instance.instanceId,
      nativeThreadId: sessionId,
    });

  const nativeThreadOwners = (
    instance: ProviderInstance,
    sessionIds: ReadonlyArray<string>,
    message: string,
  ) =>
    projections
      .getNativeThreadOwners({
        driver: instance.driverKind,
        providerInstanceId: instance.instanceId,
        nativeThreadIds: sessionIds,
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new AcpRegistryOperationError({ reason: "session_import_failed", message, cause }),
        ),
      );

  const findImportedThread = (
    instance: ProviderInstance,
    sessionId: string,
    message: string,
    reason: "session_import_failed" | "session_delete_failed",
  ) => {
    const threadId = importedThreadId(instance, sessionId);
    return projections.getThreadShell(threadId).pipe(
      Effect.map((thread) => (thread === null ? null : threadId)),
      Effect.mapError((cause) => new AcpRegistryOperationError({ reason, message, cause })),
    );
  };

  const launchImport = Effect.fn("AcpSessionImport.launchImport")(function* (input: {
    readonly instance: ProviderInstance;
    readonly projectId: ProjectId;
    readonly sessionId: string;
    readonly title: string | null | undefined;
    readonly updatedAt: string | null | undefined;
    readonly createdBy: OrchestrationV2Actor;
    readonly creationSource: OrchestrationV2CreationSource;
  }) {
    const { instance } = input;
    const threadId = importedThreadId(instance, input.sessionId);
    const provider = (yield* providerRegistry.getProviders).find(
      (candidate) => candidate.instanceId === instance.instanceId,
    );
    const model =
      provider?.models.find((candidate) => candidate.isDefault)?.slug ??
      provider?.models[0]?.slug ??
      "default";
    const commandId = CommandId.make(yield* crypto.randomUUIDv4().pipe(Effect.orDie));
    const launched = yield* Effect.result(
      startup.enqueueCommand(
        threadLaunch.launch({
          commandId,
          threadId,
          projectId: input.projectId,
          title: input.title ?? "Imported ACP session",
          modelSelection: { instanceId: instance.instanceId, model },
          runtimeMode: "approval-required",
          interactionMode: "default",
          workspaceStrategy: { type: "root" },
          importedNativeThread: {
            ref: {
              driver: instance.driverKind,
              nativeId: input.sessionId,
              strength: "strong",
            },
            metadata: {
              itemIdentityVersion: 2,
              ...(input.title === undefined ? {} : { title: input.title }),
              ...(input.updatedAt === undefined ? {} : { updatedAt: input.updatedAt }),
            },
          },
          createdBy: input.createdBy,
          creationSource: input.creationSource,
        }),
      ),
    );
    if (Result.isFailure(launched)) {
      const racedImport = yield* findImportedThread(
        instance,
        input.sessionId,
        "Could not inspect the imported ACP session after launch failed.",
        "session_import_failed",
      );
      if (racedImport !== null) return { threadId, imported: false } as const;
      return yield* new AcpRegistryOperationError({
        reason: "session_import_failed",
        message: "Could not create a T3 thread for the ACP session.",
        cause: launched.failure,
      });
    }
    return { threadId, imported: true } as const;
  });

  const listSessions = Effect.fn("AcpSessionImport.listSessions")(function* (
    input: AcpRegistryListSessionsInput,
  ) {
    const project = yield* resolveProject(input.projectId);
    const { instance, manager } = yield* resolveSessionManagement(input.instanceId);
    const listed = yield* manager.listSessions({
      cwd: project.workspaceRoot,
      ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
    });
    const owners = yield* nativeThreadOwners(
      instance,
      listed.sessions.map((session) => session.sessionId),
      "Could not inspect existing imported ACP sessions.",
    );
    return {
      ...listed,
      sessions: listed.sessions.map((session) => {
        const owner = owners.get(session.sessionId);
        return {
          ...session,
          importedThreadId: owner === undefined || owner.deleted ? null : owner.threadId,
        };
      }),
    };
  });

  const importSession = Effect.fn("AcpSessionImport.importSession")(function* (
    input: AcpRegistryImportSessionInput,
  ) {
    return yield* runtimeCoordinator.withSessionMutation(
      Effect.gen(function* () {
        yield* resolveProject(input.projectId);
        const { instance } = yield* resolveSessionManagement(input.instanceId);
        const providerSnapshot = yield* instance.snapshot.getSnapshot;
        if (
          providerSnapshot.nativeSessions?.canLoad !== true &&
          providerSnapshot.nativeSessions?.canResume !== true
        ) {
          return yield* new AcpRegistryOperationError({
            reason: "session_resume_unsupported",
            message: "The ACP agent cannot load or resume native sessions.",
          });
        }
        const owner = (yield* nativeThreadOwners(
          instance,
          [input.sessionId],
          "Could not inspect the imported ACP session mapping.",
        )).get(input.sessionId);
        if (owner !== undefined && !owner.deleted) {
          return { threadId: owner.threadId, imported: false } as const;
        }
        return yield* launchImport({
          instance,
          projectId: input.projectId,
          sessionId: input.sessionId,
          title: input.title,
          updatedAt: input.updatedAt,
          createdBy: "user",
          creationSource: "web",
        });
      }),
    );
  });

  const deleteSession = Effect.fn("AcpSessionImport.deleteSession")(function* (
    input: AcpRegistryDeleteSessionInput,
  ) {
    return yield* runtimeCoordinator.withSessionMutation(
      Effect.gen(function* () {
        const project = yield* resolveProject(input.projectId);
        const { instance, manager } = yield* resolveSessionManagement(input.instanceId);
        const snapshot = yield* instance.snapshot.getSnapshot;
        if (snapshot.nativeSessions?.canDelete !== true) {
          return yield* new AcpRegistryOperationError({
            reason: "session_delete_unsupported",
            message: "The ACP agent does not advertise session deletion.",
          });
        }
        const importedThread = yield* findImportedThread(
          instance,
          input.sessionId,
          "Could not inspect the imported ACP session mapping.",
          "session_delete_failed",
        );
        if (importedThread !== null) {
          return yield* new AcpRegistryOperationError({
            reason: "session_delete_failed",
            message: "Delete the imported T3 thread before deleting its native ACP session.",
          });
        }
        yield* manager.deleteSession({
          cwd: project.workspaceRoot,
          sessionId: input.sessionId,
        });
        return { deleted: true } as const;
      }),
    );
  });

  const pollInterval = settingsService.getSettings.pipe(
    Effect.map(
      (settings) => resolveServerBackgroundActivitySettings(settings).providerHealthRefreshInterval,
    ),
    Effect.orElseSucceed(() => DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL),
  );

  const autoImportManager = Effect.fnUntraced(function* (instance: ProviderInstance) {
    const manager = instance.acpSessionManagement;
    if (!instance.enabled || manager?.autoImportSessions !== true) return undefined;
    const nativeSessions = (yield* instance.snapshot.getSnapshot).nativeSessions;
    return nativeSessions?.canList === true && (nativeSessions.canLoad || nativeSessions.canResume)
      ? manager
      : undefined;
  });

  const autoImport = Effect.fn("AcpSessionImport.autoImport")(function* (
    instance: ProviderInstance,
    projectId: ProjectId,
    session: AcpRegistrySession,
  ) {
    return yield* runtimeCoordinator.withSessionMutation(
      Effect.gen(function* () {
        // A scan can outlive a settings change; only the current instance may import.
        const current = yield* providerInstances.getInstance(instance.instanceId);
        if (current !== instance || current.acpSessionManagement?.autoImportSessions !== true) {
          return;
        }
        const owners = yield* nativeThreadOwners(
          instance,
          [session.sessionId],
          "Could not inspect the imported ACP session mapping.",
        );
        if (owners.has(session.sessionId)) return;
        yield* launchImport({
          instance,
          projectId,
          sessionId: session.sessionId,
          title: session.title,
          updatedAt: session.updatedAt,
          createdBy: "system",
          creationSource: "server",
        });
      }),
    );
  });

  const sweepInstance = Effect.fn("AcpSessionImport.sweepInstance")(function* (input: {
    readonly instance: ProviderInstance;
    readonly manager: AcpSessionManagement;
    readonly roots: Arr.NonEmptyReadonlyArray<string>;
    readonly projectIdsByRoot: ReadonlyMap<string, ProjectId>;
    readonly interval: Duration.Duration;
  }) {
    const { instance } = input;
    const now = yield* Clock.currentTimeMillis;
    const state = pollStates.get(instance);
    if (state !== undefined && now < state.nextAttemptAt) return;
    const recordFailure = (cause: unknown) => {
      const failures = (state?.failures ?? 0) + 1;
      pollStates.set(instance, {
        failures,
        nextAttemptAt:
          now +
          Duration.toMillis(input.interval) * 2 ** Math.min(failures - 1, MAX_BACKOFF_EXPONENT),
      });
      return Effect.logWarning("ACP session auto-import failed", {
        instanceId: instance.instanceId,
        failures,
        cause,
      });
    };

    const scanned = yield* Effect.result(input.manager.scanSessions(input.roots));
    if (Result.isFailure(scanned)) return yield* recordFailure(scanned.failure);
    if (Option.isNone(scanned.success)) return;

    const candidates = new Map<
      string,
      { readonly session: AcpRegistrySession; readonly projectId: ProjectId }
    >();
    for (const session of scanned.success.value) {
      // An imported thread resumes at the project root, so only root sessions qualify.
      const projectId = input.projectIdsByRoot.get(normalizeProjectPathForComparison(session.cwd));
      if (projectId !== undefined && !candidates.has(session.sessionId)) {
        candidates.set(session.sessionId, { session, projectId });
      }
    }
    const owners = yield* Effect.result(
      nativeThreadOwners(
        instance,
        [...candidates.keys()],
        "Could not inspect existing imported ACP sessions.",
      ),
    );
    if (Result.isFailure(owners)) return yield* recordFailure(owners.failure);

    let failure: AcpRegistryOperationError | undefined;
    for (const [sessionId, candidate] of candidates) {
      if (owners.success.has(sessionId)) continue;
      const imported = yield* Effect.result(
        autoImport(instance, candidate.projectId, candidate.session),
      );
      if (Result.isFailure(imported)) {
        failure = imported.failure;
        yield* Effect.logWarning("Could not auto-import an ACP session", {
          instanceId: instance.instanceId,
          sessionId,
          cause: imported.failure,
        });
      }
    }
    if (failure !== undefined) return yield* recordFailure(failure);
    pollStates.delete(instance);
  });

  const sweep = Effect.gen(function* () {
    const interval = yield* pollInterval;
    if (!Duration.isPositive(interval)) return;
    if (!(yield* backgroundPolicy.shouldRunOpportunisticWork)) return;
    const eligible = (yield* Effect.forEach(yield* providerInstances.listInstances, (instance) =>
      autoImportManager(instance).pipe(
        Effect.map((manager) => (manager === undefined ? [] : [{ instance, manager }])),
      ),
    )).flat();
    if (eligible.length === 0) return;
    const projectIdsByRoot = new Map<string, ProjectId>();
    const roots: Array<string> = [];
    for (const project of yield* projectService.listShells()) {
      const key = normalizeProjectPathForComparison(project.workspaceRoot);
      if (projectIdsByRoot.has(key)) continue;
      projectIdsByRoot.set(key, project.id);
      roots.push(project.workspaceRoot);
    }
    if (!Arr.isReadonlyArrayNonEmpty(roots)) return;
    yield* Effect.forEach(
      eligible,
      ({ instance, manager }) =>
        sweepInstance({ instance, manager, roots, projectIdsByRoot, interval }),
      { discard: true },
    );
  }).pipe(
    Effect.withSpan("AcpSessionImport.sweep"),
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("ACP session auto-import sweep failed", { cause: Cause.pretty(cause) }),
    ),
  );

  const start = Effect.fn("AcpSessionImport.start")(function* () {
    yield* forkParked(
      Effect.forever(
        sweep.pipe(
          Effect.andThen(pollInterval),
          Effect.flatMap((interval) =>
            Effect.sleep(
              Duration.isPositive(interval) ? interval : DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL,
            ),
          ),
        ),
      ),
    );
  });

  return AcpSessionImport.of({
    resolveProject,
    resolveSessionManagement,
    listSessions,
    importSession,
    deleteSession,
    sweep,
    start,
  });
});

export const layer = Layer.effect(AcpSessionImport, make);
