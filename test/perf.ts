import { padStart } from "alcalzone-shared/strings";
import fs from "fs-extra";
import { JsonlDB } from "../src";

process.on("unhandledRejection", (r) => {
	debugger;
});

async function testMedium() {
	const testDB: JsonlDB<any> = new JsonlDB("test.jsonl", {
		autoCompress: {
			sizeFactor: 2,
			sizeFactorMinimumSize: 5000,
		},
		ignoreReadErrors: true,
		throttleFS: {
			intervalMs: 60000,
			maxBufferedCommands: 100,
		},
	});

	function makeObj(i: number) {
		return {
			type: "state",
			common: {
				name: i.toString(),
				read: true,
				write: true,
				role: "state",
				type: "number",
			},
			native: {},
		};
	}

	const NUM_PASSES = 10;
	const NUM_OBJECTS = 100000;
	let total: number = 0;

	console.log("start test MEDIUM");

	for (let pass = 1; pass <= NUM_PASSES; pass++) {
		await fs.remove("test.jsonl");

		await testDB.open();

		const start = Date.now();
		for (let i = 0; i < NUM_OBJECTS; i++) {
			const key = `benchmark.0.test${i}`;
			const value = makeObj(i);
			testDB.set(key, value);
		}

		await testDB.close();

		const time = Date.now() - start;
		total += time;

		process.stdout.write(".");
	}

	await fs.remove("test.jsonl");

	process.stdout.write("\n\n");

	console.log(`${NUM_PASSES}x, ${NUM_OBJECTS} objects`);
	console.log(`  ${(total / NUM_PASSES).toFixed(2)} ms / attempt`);
	console.log(`  ${((NUM_OBJECTS / total) * 1000).toFixed(2)} changes/s`);
	console.log();
	console.log();
}

async function testSmall() {
	const testDB: JsonlDB<any> = new JsonlDB("test.jsonl", {
		autoCompress: { onClose: false },
		throttleFS: {
			intervalMs: 1000,
		},
	});

	// add a shitton of values
	const NUM_PASSES = 10;
	const NUM_KEYS = 1000;
	const NUM_CHANGES = 100000;
	let total: number = 0;

	console.log("start test SMALL");

	for (let pass = 1; pass <= NUM_PASSES; pass++) {
		await fs.remove("test.jsonl");

		await testDB.open();

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
		await testDB.close();

		const time = Date.now() - start;
		total += time;

		process.stdout.write(".");
	}

	await fs.remove("test.jsonl");

	process.stdout.write("\n\n");

	console.log(`${NUM_PASSES}x, ${NUM_KEYS} keys, ${NUM_CHANGES} changes`);
	console.log(`  ${(total / NUM_PASSES).toFixed(2)} ms / attempt`);
	console.log(`  ${((NUM_CHANGES / total) * 1000).toFixed(2)} changes/s`);
	console.log();
	console.log();
}

testSmall()
	.then(testMedium)
	.catch(console.error)
	.finally(() => fs.remove("test.jsonl"))
	.catch(() => {
		/* ignore */
	});
