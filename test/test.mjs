import { JsonlDB } from "@alcalzone/jsonl-db";

const testDB = new JsonlDB("test.jsonl", {
	autoCompress: { onClose: false },
	throttleFS: {
		intervalMs: 10000,
	},
});

(async () => {
	await testDB.open();
	// add a shitton of values
	console.time("create values");
	const MAX_NODES = 10;
	for (let pass = 1; pass <= 10; pass++) {
		for (let nodeId = 1; nodeId <= MAX_NODES; nodeId++) {
			for (let ccId = 1; ccId <= 10; ccId++) {
				for (let endpoint = 0; endpoint <= 10; endpoint++) {
					for (const property of ["a", "b", "c", "d", "e"]) {
						const key = `${nodeId}-${ccId}-${endpoint}-${property}`;
						if (Math.random() < 0.15) {
							testDB.delete(key);
						} else {
							testDB.set(key, Math.random() * 100);
						}
					}
				}
			}
		}
	}
	await testDB.close();
	console.timeEnd("create values");

	console.time("open values");
	await testDB.open();
	console.log(testDB.size);
	console.timeEnd("open values");

	await testDB.close();

	// await fs.remove("test.jsonl");
})().catch(() => {
	/* ignore */
});
