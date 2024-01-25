import { describe, expect, it } from "vitest";
import { Signal } from "./signal";

describe("signal", () => {
	it("can be awaited and resolves immediately when set", async () => {
		const signal = new Signal();
		signal.set();
		await signal;
	});

	it("can be awaited and resolves when set", async () => {
		const signal = new Signal();
		setTimeout(() => signal.set(), 100);
		await signal;
	});

	it("does not resolve when awaited after being reset", async () => {
		const signal = new Signal();
		signal.set();
		signal.reset();

		const result = await Promise.race([
			signal.then(() => "resolved"),
			new Promise((resolve) => setTimeout(resolve, 100)).then(
				() => "timeout",
			),
		]);

		expect(result).toBe("timeout");
	});

	it("can be re-awaited multiple times in sequence", async () => {
		const signal = new Signal();

		setTimeout(() => signal.set(), 100);
		await signal;
		signal.reset();

		setTimeout(() => signal.set(), 100);
		await signal;
		signal.reset();

		setTimeout(() => signal.set(), 100);
		await signal;
		signal.reset();
	});
});
