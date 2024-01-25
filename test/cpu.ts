import { JsonlDB } from "../src";

async function main() {
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

	await testDB.open();
	for (let i = 0; i < 100; i++) {
		testDB.set(`benchmark.0.test${i}`, i);
	}

	await new Promise((resolve) => setTimeout(resolve, 20000));

	await testDB.close();
}
void main();
