/** Environment variables that activate the task-worker gate. Either one set to exactly "1" selects worker mode. */
export const TASK_WORKER_ENV_VARS = Object.freeze(["PI_TASK_WORKER", "WOLFPACK_TASK_WORKER"] as const);

export function taskWorkerModeEnabled(env: Readonly<Record<string, string | undefined>> = process.env): boolean {
	return TASK_WORKER_ENV_VARS.some((name) => env[name] === "1");
}
