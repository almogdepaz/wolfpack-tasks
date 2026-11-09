import { describe, expect, test } from "bun:test";
import { closeSync, copyFileSync, existsSync, fstatSync, fsyncSync, mkdtempSync, openSync, readFileSync, rmSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { loadSelectedPiRuntime, piRuntimeSource } from "./pi-runtime-source";

const entriesOnDisk = (path: string): any[] => readFileSync(path, "utf8").trim().split("\n").map(line => JSON.parse(line));

describe.skipIf(!piRuntimeSource)("actual selected Pi runtime: identity/persistence/queue characterization (not execution proof)", () => {
  test("planned persistent file and RAM/current-branch entries do not prove persistence", async () => {
    const { sdk, version } = await loadSelectedPiRuntime(), root = mkdtempSync("/tmp/pi-owner-characterization-");
    try {
      const manager = sdk.SessionManager.create(root, join(root, "sessions"));
      const identity = manager.getSessionId(), path = manager.getSessionFile();
      expect(typeof identity).toBe("string"); expect(manager.getHeader().id).toBe(identity); expect(typeof path).toBe("string");
      const entryId = manager.appendCustomMessageEntry("pi-tasks-event", "not yet persisted", true, { taskId: "task", eventId: "event" });
      expect(manager.getEntries().some((entry: any) => entry.id === entryId)).toBe(true);
      expect(manager.getBranch().some((entry: any) => entry.id === entryId)).toBe(true);
      expect(existsSync(path)).toBe(false);
      manager.appendMessage({ role: "user", content: "first real conversation message", timestamp: Date.now() });
      expect(existsSync(path)).toBe(true);
      expect(entriesOnDisk(path).some(entry => entry.id === entryId)).toBe(true);
      const reopened = sdk.SessionManager.open(path, join(root, "sessions"), root);
      expect(reopened.getSessionId()).toBe(identity); expect(reopened.getSessionFile()).toBe(path);
      console.log(`Pi ${version}: RAM entry precedes file; a user message flushes the existing entry`);
    } finally { rmSync(root, { recursive: true, force: true }); }
  });

  test("new/fork create different IDs; copied history preserves ID but changes file identity; tree excludes old evidence", async () => {
    const { sdk } = await loadSelectedPiRuntime(), root = mkdtempSync("/tmp/pi-identity-characterization-");
    try {
      const manager = sdk.SessionManager.create(root, join(root, "sessions"));
      const rootEntry = manager.appendMessage({ role: "user", content: "root", timestamp: Date.now() });
      const eventId = manager.appendCustomMessageEntry("pi-tasks-event", "assignment", true, { taskId: "task", eventId: "event" });
      const wakeId = manager.appendCustomMessageEntry("pi-tasks-wake", "wake", false, { taskId: "task", eventId: "event" });
      const oldId = manager.getSessionId(), oldFile = manager.getSessionFile();
      const branchFile = manager.createBranchedSession(wakeId), fork = sdk.SessionManager.open(branchFile, join(root, "sessions"), root);
      expect(fork.getSessionId()).not.toBe(oldId); expect(fork.getSessionFile()).not.toBe(oldFile);
      expect(fork.getEntries().some((entry: any) => entry.id === eventId)).toBe(true);
      const copied = join(root, "copied.jsonl"); copyFileSync(oldFile, copied);
      const imported = sdk.SessionManager.open(copied, join(root, "sessions"), root);
      expect(imported.getSessionId()).toBe(oldId); expect(imported.getSessionFile()).not.toBe(oldFile);
      expect(statSync(copied).ino).not.toBe(statSync(oldFile).ino);
      const original = sdk.SessionManager.open(oldFile, join(root, "sessions"), root);
      original.branch(rootEntry);
      expect(original.getSessionId()).toBe(oldId);
      expect(original.getEntries().some((entry: any) => entry.id === wakeId)).toBe(true);
      expect(entriesOnDisk(oldFile).some(entry => entry.id === wakeId)).toBe(true);
      expect(original.getBranch().some((entry: any) => entry.id === eventId || entry.id === wakeId)).toBe(false);
      original.newSession(); expect(original.getSessionId()).not.toBe(oldId);
      expect(sdk.SessionManager.inMemory(root).getSessionFile()).toBeUndefined();
    } finally { rmSync(root, { recursive: true, force: true }); }
  });

  test("native read-only descriptors can synchronize actual persisted Pi entries without rewriting the file", async () => {
    const { sdk } = await loadSelectedPiRuntime(), root = mkdtempSync("/tmp/pi-disk-characterization-");
    try {
      const manager = sdk.SessionManager.create(root, join(root, "sessions"));
      manager.appendMessage({ role: "user", content: "persist the session normally", timestamp: Date.now() });
      const id = manager.appendCustomMessageEntry("pi-tasks-event", "fixture", true, { taskId: "task", eventId: "event", attemptId: "attempt" });
      const path = manager.getSessionFile(), fd = openSync(path, "r"), directory = openSync(dirname(path), "r");
      try {
        const before = fstatSync(fd, { bigint: true }), data = readFileSync(fd, "utf8");
        fsyncSync(fd); fsyncSync(directory);
        const after = fstatSync(fd, { bigint: true });
        expect(after.dev).toBe(before.dev); expect(after.ino).toBe(before.ino); expect(after.size).toBe(before.size);
        expect(readFileSync(path, "utf8")).toBe(data);
        const disk = data.trim().split("\n").map((line: string) => JSON.parse(line));
        expect(disk[0].id).toBe(manager.getSessionId());
        expect(disk.find((entry: any) => entry.id === id)).toEqual(manager.getEntry(id));
        expect(manager.getBranch().some((entry: any) => entry.id === id)).toBe(true);
      } finally { closeSync(fd); closeSync(directory); }
    } finally { rmSync(root, { recursive: true, force: true }); }
  });

  test("real public extension sendMessage can return during agent_settled before its wake entry exists", async () => {
    const { sdk, ai } = await loadSelectedPiRuntime(), root = mkdtempSync("/tmp/pi-queue-characterization-");
    let session: any;
    try {
      const manager = sdk.SessionManager.create(root, join(root, "sessions"));
      const runtime = await sdk.ModelRuntime.create({ authPath: join(root, "auth.json"), modelsPath: null, modelsStorePath: join(root, "models.json"), allowModelNetwork: false, refreshOnCreate: false });
      const faux = ai.fauxProvider({ provider: "recovery-fixture", models: [{ id: "offline-model" }] });
      runtime.registerNativeProvider(faux.provider); faux.setResponses([ai.fauxAssistantMessage("first"), ai.fauxAssistantMessage("after queued wake")]);
      let emissionCount = 0, immediateWakeCount: number | undefined, settled = 0;
      const loader = new sdk.DefaultResourceLoader({ cwd: root, agentDir: root, noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        extensionFactories: [(pi: any) => { pi.on("agent_settled", () => {
          if (++settled !== 1) return;
          emissionCount++;
          pi.sendMessage({ customType: "pi-tasks-wake", content: "fixture wake", display: false, details: { taskId: "task", eventId: "event" } }, { triggerTurn: true, deliverAs: "followUp" });
          immediateWakeCount = manager.getEntries().filter((entry: any) => entry.type === "custom_message" && entry.customType === "pi-tasks-wake").length;
        }); }],
      });
      await loader.reload();
      ({ session } = await sdk.createAgentSession({ cwd: root, agentDir: root, resourceLoader: loader, sessionManager: manager, modelRuntime: runtime, model: faux.getModel(), settingsManager: sdk.SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } }), noTools: "all" }));
      await session.bindExtensions({}); await session.prompt("fixture initial prompt"); await session.waitForIdle();
      expect(immediateWakeCount).toBe(0); expect(emissionCount).toBe(1);
      expect(manager.getEntries().filter((entry: any) => entry.type === "custom_message" && entry.customType === "pi-tasks-wake")).toHaveLength(1);
      expect(entriesOnDisk(manager.getSessionFile()).filter(entry => entry.type === "custom_message" && entry.customType === "pi-tasks-wake")).toHaveLength(1);
      expect(faux.state.callCount).toBe(2); // Invocation of an offline fixture model, never proof of tool success.
    } finally { session?.dispose(); rmSync(root, { recursive: true, force: true }); }
  }, 15_000);
});
