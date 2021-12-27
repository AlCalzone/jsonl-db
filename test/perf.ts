import { padStart } from "alcalzone-shared/strings";
import fs from "fs-extra";
import { JsonlDB } from "../src";

process.on("unhandledRejection", (r) => {
	debugger;
});

const testDB: JsonlDB<any> = new JsonlDB("test.jsonl", {
	autoCompress: { onClose: false },
	throttleFS: {
		intervalMs: 1000,
	},
});

(async () => {
	// add a shitton of values
	const NUM_PASSES = 10;
	const NUM_KEYS = 1000;
	const NUM_CHANGES = 100000;
	let total: number = 0;

	// console.time("open");
	// console.timeEnd("open");

	for (let pass = 1; pass <= NUM_PASSES; pass++) {
		await fs.remove("test.jsonl");

		await testDB.open();

		console.log(`start ${pass}`);

		const start = Date.now();
		for (let i = 0; i < NUM_CHANGES; i++) {
			const key = `k${padStart(
				Math.round(Math.random() * NUM_KEYS).toString(),
				5,
				"0",
			)}`;
			if (Math.random() < 0.15) {
				testDB.delete(key);
			} else {
				testDB.set(key, Math.random() * 100);
			}
		}
		console.log("close");
		await testDB.close();

		const time = Date.now() - start;
		total += time;

		console.log(`end ${pass}`);
	}
	// console.time("close");
	// await testDB.close();
	// 	console.timeEnd("close");

	console.log(`${NUM_PASSES}x, ${NUM_KEYS} keys, ${NUM_CHANGES} changes`);
	console.log(`  ${(total / NUM_PASSES).toFixed(2)} ms / attempt`);
	console.log(`  ${((NUM_CHANGES / total) * 1000).toFixed(2)} changes/s`);

	process.exit(0);

	// console.time("open values");
	// await testDB.open();
	// console.log(testDB.size);
	// console.timeEnd("open values");

	// await testDB.close();

	// await fs.remove("test.jsonl");
})().catch(() => {
	/* ignore */
});
