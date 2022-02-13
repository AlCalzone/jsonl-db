import { wait } from "alcalzone-shared/async";
import * as fs from "fs-extra";
import path from "path";
import { TestFS } from "../../test/testFs";
import { JsonlDB } from "./db";

let mockAppendFileThrottle = 0;
let mockMoveFileThrottle = 0;

jest.mock("fs-extra", () => {
	const originalFS = jest.requireActual("fs-extra");
	// eslint-disable-next-line @typescript-eslint/no-var-requires
	const wait = require("alcalzone-shared/async").wait;
	return {
		__esModule: true, // Use it when dealing with esModules
		...originalFS,
		appendFile: jest.fn().mockImplementation(async (...args) => {
			if (mockAppendFileThrottle > 0) {
				await wait(mockAppendFileThrottle);
			}
			return originalFS.appendFile(...args);
		}),
		move: jest.fn().mockImplementation(async (...args) => {
			if (mockMoveFileThrottle > 0) {
				await wait(mockMoveFileThrottle);
			}
			return originalFS.move(...args);
		}),
		createReadStream: jest.fn().mockImplementation((path, options) => {
			// eslint-disable-next-line @typescript-eslint/no-var-requires
			const { PassThrough } = require("stream");
			const { /*fd,*/ encoding } = options;
			// const file = fd
			// 	? originalFS.readFileSync(fd, encoding)
			// 	: originalFS.readFileSync(path, encoding);
			const file = originalFS.readFileSync(path, encoding);
			const ret = new PassThrough();
			ret.write(file, encoding, () => {
				ret.end();
			});
			return ret;
		}),
	};
});
function assertEqual<
	T1 extends {
		keys(): IterableIterator<string>;
		has(key: string): boolean;
		get(key: string): any;
	},
	T2 extends {
		keys(): IterableIterator<string>;
		has(key: string): boolean;
		get(key: string): any;
	},
>(one: T1, two: T2) {
	for (const key of one.keys()) {
		expect(two.has(key)).toBeTrue();
		expect(two.get(key)).toBe(one.get(key));
	}
}

describe("lib/db", () => {
	describe("constructor", () => {
		describe("validates autoCompress options", () => {
			it("sizeFactor <= 1", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							autoCompress: {
								sizeFactor: 0.9,
							},
						}),
				).toThrowError("sizeFactor");
			});

			it("minimumSize <= 0", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							autoCompress: {
								sizeFactorMinimumSize: -1,
							},
						}),
				).toThrowError("sizeFactorMinimumSize");
			});

			it("intervalMs < 10", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							autoCompress: {
								intervalMs: 9,
							},
						}),
				).toThrowError("intervalMs");
			});

			it("intervalMinChanges < 10", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							autoCompress: {
								intervalMinChanges: 0,
							},
						}),
				).toThrowError("intervalMinChanges");
			});
		});

		describe("validates throttleFS options", () => {
			it("intervalMs < 0", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							throttleFS: {
								intervalMs: -1,
							},
						}),
				).toThrowError("intervalMs");
			});

			it("maxBufferedCommands < 0", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							throttleFS: {
								intervalMs: 0,
								maxBufferedCommands: -1,
							},
						}),
				).toThrowError("maxBufferedCommands");
			});
		});

		describe("validates lockfile options", () => {
			it("staleMs < 2000", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								staleMs: 1999,
							},
						}),
				).toThrowError("staleMs");
			});

			it("updateMs < 1000", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								updateMs: 999,
							},
						}),
				).toThrowError("updateMs");
			});

			it("updateMs > staleMs/2", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								staleMs: 5000,
								updateMs: 10001,
							},
						}),
				).toThrowError("updateMs");
			});

			it("retries < 0", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								retries: -1,
							},
						}),
				).toThrowError("retries");
			});

			it("retries > 10", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								retries: 11,
							},
						}),
				).toThrowError("retries");
			});

			it("retryMinTimeoutMs < 100", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								retryMinTimeoutMs: 99,
							},
						}),
				).toThrowError("retryMinTimeoutMs");
			});

			it("lockfileDirectory and lockfile.directory both present", () => {
				expect(
					() =>
						new JsonlDB("foo", {
							lockfile: {
								directory: "/lock/dir",
							},
							lockfileDirectory: "/lock/dir2",
						}),
				).toThrowError("lockfileDirectory");
			});
		});
	});

	describe("open()", () => {
		let testFS: TestFS;
		let testFSRoot: string;
		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();

			try {
				await testFS.remove();
				await testFS.create({
					yes:
						// Final newline omitted on purpose
						'{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n{"k":"key1"}',
					emptyLines:
						'\n{"k":"key1","v":1}\n\n\n{"k":"key2","v":"2"}\n\n',
					broken: `{"k":"key1","v":1}\n{"k":,"v":1}\n`,
					broken2: `{"k":"key1","v":1}\n{"k":"key2","v":}\n`,
					broken3: `{"k":"key1"\n`,
					reviver: `
{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key1"}
{"k":"key1","v":true}`,
				});
			} catch (e) {
				debugger;
			}
		});
		afterEach(async () => {
			try {
				await testFS.remove();
			} catch (e) {
				debugger;
			}
		});

		it("sets the isOpen property to true", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "yes"));
			await db.open();
			expect(db.isOpen).toBeTrue();
			await db.close();
		});

		it("checks if the given file exists and creates it if it doesn't", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "no"));
			await db.open();
			await db.close();
		});

		it("also creates leading directories if they don't exist", async () => {
			const db = new JsonlDB(
				path.join(testFSRoot, "this/path/does/not/exist"),
			);
			await db.open();
			await db.close();
		});

		it("also creates leading directories for the lockfiles if they don't exist", async () => {
			const lockfileDirectory = path.join(
				testFSRoot,
				"this/path/does/not/exist/either",
			);
			const db = new JsonlDB(path.join(testFSRoot, "lockfile"), {
				lockfileDirectory,
			});
			await db.open();
			await db.close();

			await expect(fs.pathExists(lockfileDirectory)).resolves.toBeTrue();
		});

		it("reads the file if it exists", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "yes"));
			await db.open();
			await db.close();
		});

		it("throws if another DB has opened the DB file at the same time", async () => {
			const db1 = new JsonlDB(path.join(testFSRoot, "yes"));
			await db1.open();

			const db2 = new JsonlDB(path.join(testFSRoot, "yes"));
			try {
				await db2.open();
				throw new Error("it did not throw");
			} catch (e: any) {
				expect(e.message).toMatch(/Failed to lock/i);
			}

			await db1.close();

			await db2.open();
			await db2.close();
		});

		it("should contain the correct data", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "yes"));
			await db.open();

			expect(db.size).toBe(1);
			expect(db.has("key1")).toBeFalse();
			expect(db.has("key2")).toBeTrue();
			expect(db.get("key2")).toBe("2");

			const spy = jest.fn();
			db.forEach(spy);
			expect(spy).toBeCalledTimes(1);
			expect(spy.mock.calls[0].slice(0, 2)).toEqual(["2", "key2"]);

			await db.close();
		});

		it("skips empty input lines", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "emptyLines"));
			await db.open();

			expect(db.has("key1")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.has("key2")).toBeTrue();
			expect(db.get("key2")).toBe("2");

			await db.close();
		});

		it("throws when the file contains invalid JSON", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "broken"));
			try {
				await db.open();
				throw new Error("it did not throw");
			} catch (e: any) {
				expect(e.message).toMatch(/invalid data/i);
				expect(e.message).toMatch("line 2");
			}
		});

		it("throws when the file contains invalid JSON (part 2)", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "broken2"));
			try {
				await db.open();
				throw new Error("it did not throw");
			} catch (e: any) {
				expect(e.message).toMatch(/invalid data/i);
				expect(e.message).toMatch("line 2");
			}
		});

		it("throws when the file contains invalid JSON (part 3)", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "broken3"));
			try {
				await db.open();
				throw new Error("it did not throw");
			} catch (e: any) {
				expect(e.message).toMatch(/invalid data/i);
				expect(e.message).toMatch("line 1");
			}
		});

		it("does not throw when the file contains invalid JSON and `ignoreReadErrors` is true", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "broken"), {
				ignoreReadErrors: true,
			});
			await expect(db.open()).toResolve();
			await db.close();
		});

		it("does not throw when the file contains invalid JSON and `ignoreReadErrors` is true (part 2)", async () => {
			const db = new JsonlDB(path.join(testFSRoot, "broken2"), {
				ignoreReadErrors: true,
			});
			await expect(db.open()).toResolve();
			await db.close();
		});

		it("transforms each value using the valueReviver function if any is passed", async () => {
			const reviver = jest.fn().mockReturnValue("eeee");
			const db = new JsonlDB(path.join(testFSRoot, "reviver"), {
				reviver,
			});
			await db.open();
			expect(reviver).toBeCalledTimes(2);
			expect(reviver).toBeCalledWith("key2", "2");
			expect(reviver).toBeCalledWith("key1", true);

			db.forEach((v) => {
				expect(v).toBe("eeee");
			});
			await db.close();
		});
	});

	describe("clear()", () => {
		const testFilename = "clear.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]:
					'{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n{"k":"key1"}\n',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await testFS.remove();
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			expect(() => db.clear()).toThrowError("not open");
		});

		it("removes all entries from the database and truncates the file", async () => {
			db.clear();
			expect(db.size).toBe(0);
			expect(db.has("key1")).toBeFalse();
			expect(db.has("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(fs.stat(testFilenameFull)).resolves.toMatchObject({
				size: 0,
			});
		});
	});

	describe("delete()", () => {
		const testFilename = "delete.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await testFS.remove();
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			expect(() => db.delete("key1")).toThrowError("not open");
		});

		it("removes the given key from the database and writes a line with an undefined value", async () => {
			expect(db.delete("key2")).toBeTrue();
			expect(db.size).toBe(1);
			expect(db.has("key1")).toBeTrue();
			expect(db.has("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.toEndWith(`{"k":"key2"}\n`);
		});

		it("removes multiple key from the database in the correct order", async () => {
			expect(db.delete("key2")).toBeTrue();
			expect(db.delete("key1")).toBeTrue();
			expect(db.size).toBe(0);
			expect(db.has("key1")).toBeFalse();
			expect(db.has("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.toEndWith(`{"k":"key2"}\n{"k":"key1"}\n`);
		});

		it("deleting a key twice does not write to the log twice", async () => {
			expect(db.delete("key2")).toBeTrue();
			expect(db.delete("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n{"k":"key2"}\n`,
			);
		});
	});

	describe("set()", () => {
		const testFilename = "set.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create();
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});

		afterEach(async () => {
			await testFS.remove();
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			expect(() => db.set("foo", 1)).toThrowError("not open");
		});

		it("adds the given key to the database and writes a line with the serialized value", async () => {
			db.set("key", true);
			expect(db.size).toBe(1);
			expect(db.has("key")).toBeTrue();

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"key","v":true}\n`,
			);
		});

		it("adds multiple keys to the database in the correct order", async () => {
			db.set("key2", true);
			db.set("key1", 1000);
			db.set("key3", "");

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"key2","v":true}\n{"k":"key1","v":1000}\n{"k":"key3","v":""}\n`,
			);
		});
	});

	describe("importJson()", () => {
		const testFilename = "import.jsonl";
		let testFilenameFull: string;
		let jsonFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			jsonFilenameFull = path.join(testFSRoot, "jsonFile");
			await testFS.create({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
				jsonFile: '{"key3": 1, "key4": true}',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await testFS.remove();
		});

		it("both versions throw when the DB is not open", async () => {
			await db.close();
			expect(() => db.importJson({})).toThrowError("not open");
			await expect(db.importJson("")).toReject();
		});

		it("the object version adds all keys and values to the database", async () => {
			db.importJson({
				foo: "bar",
				baz: "inga",
				"1": 1,
			});
			// Force the stream to be flushed
			await db.close();

			// The order changes because Object.entries reads the entries in a different order
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"1","v":1}
{"k":"foo","v":"bar"}
{"k":"baz","v":"inga"}
`,
			);
		});

		it("the file version asynchronously adds all keys and values to the database", async () => {
			await db.importJson(jsonFilenameFull);
			// Force the stream to be flushed
			await db.close();

			// The order changes because Object.entries reads the entries in a different order
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key3","v":1}
{"k":"key4","v":true}
`,
			);
		});
	});

	describe("exportJson()", () => {
		const testFilename = "export.jsonl";
		let testFilenameFull: string;
		let jsonFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			jsonFilenameFull = path.join(testFSRoot, "jsonFile");
			await testFS.create({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
				jsonFile: '{"key3": 1, "key4": true}',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			if (db) await db.close();
			await testFS.remove();
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			await expect(db.exportJson(jsonFilenameFull)).toReject();
		});

		it("overwrites the given file with the DB contents as valid JSON", async () => {
			await db.exportJson(jsonFilenameFull);
			await expect(fs.readFile(jsonFilenameFull, "utf8")).resolves.toBe(
				`{"key1":1,"key2":"2"}\n`,
			);
		});

		it("honors the JSON formatting options", async () => {
			await db.exportJson(jsonFilenameFull, { spaces: "\t" });
			await expect(fs.readFile(jsonFilenameFull, "utf8")).resolves.toBe(
				`{
	"key1": 1,
	"key2": "2"
}\n`,
			);
		});
	});

	describe("close()", () => {
		const testFilename = "close.jsonl";
		let testFilenameFull: string;
		// The basic functionality is tested in the other suites
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create();
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await testFS.remove();
		});

		it("may be called twice", async () => {
			await db.close();
			await db.close();
		});

		it("sets the isOpen property to false", async () => {
			await db.close();
			expect(db.isOpen).toBeFalse();
		});
	});

	describe("dump()", () => {
		const testFilename = "dump.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let dumpdb: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: "",
				[testFilename + ".dump"]: "",
			});
			db = new JsonlDB(testFilenameFull);
			dumpdb = new JsonlDB(testFilenameFull + ".dump");
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			await dumpdb.close();
			await testFS.remove();
			mockAppendFileThrottle = 0;
		});

		it("writes a compressed version of the database", async () => {
			for (let i = 1; i < 20; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
				}
			}
			await db.dump();

			await dumpdb.close();
			await dumpdb.open();
			assertEqual(db, dumpdb);
		});

		it("when additional data is written during the dump, it is also dumped", async () => {
			// simulate a slow FS
			mockAppendFileThrottle = 50;
			let dumpPromise: Promise<void>;
			// 10 entries are written before the dump
			// Afterwards: write 11, 20ms pause, delete 11, 20ms pause, write 13, dump done
			for (let i = 1; i <= 13; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
				}
				if (i === 10) dumpPromise = db.dump();
				if (i > 10) await wait(20);
			}
			await dumpPromise!;

			await dumpdb.close();
			await dumpdb.open();
			assertEqual(db, dumpdb);
		});

		it("blocks the close() call", async () => {
			for (let i = 1; i < 20; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
				}
			}

			// simulate a slow FS
			mockAppendFileThrottle = 50;
			// dump without waiting
			db.dump();
			// wait a bit, so the files are being opened
			await wait(20);
			// and write something that will be put into the dump backlog
			db.set("21", 21);

			await db.close();
			// The dumpdb must be opened before the db, because the open call will delete the old dump
			await dumpdb.open();
			await db.open();
			assertEqual(db, dumpdb);
		});
	});

	describe("compress()", () => {
		const testFilename = "compress.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			await testFS.remove();
			mockMoveFileThrottle = 0;
			mockAppendFileThrottle = 0;
		});

		it("overwrites the append-only file with a compressed version", async () => {
			db.set("key3", 3);
			db.delete("key2");
			db.set("key3", 3.5);

			await db.compress();
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);
			await expect(
				fs.pathExists(testFilenameFull + ".dump"),
			).resolves.toBeFalse();
			await expect(
				fs.pathExists(testFilenameFull + ".bak"),
			).resolves.toBeFalse();
		});

		it("after compresing, writing works as usual", async () => {
			db.set("key3", 3);
			db.delete("key2");
			db.set("key3", 3.5);
			await db.compress();

			db.set("key2", 1);
			// Force flush
			await db.close();
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n{"k":"key2","v":1}\n',
			);
		});

		it("does not do anything while the DB is being closed", async () => {
			db.set("key3", 3);
			await wait(30);
			db.delete("key2");
			db.set("key3", 3.5);
			const closePromise = db.close();
			await db.compress();
			await closePromise;

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n{"k":"key3","v":3}\n{"k":"key2"}\n{"k":"key3","v":3.5}\n',
			);
		});

		it("works correctly while the DB is being compressed already", async () => {
			db.set("key3", 3);
			db.delete("key2");
			db.set("key3", 3.5);

			// simulate slow FS
			mockMoveFileThrottle = 10;
			mockAppendFileThrottle = 10;

			const compressPromise = db.compress();
			await db.compress();
			await compressPromise;

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);
		});

		it("when additional data is written while the files are moved, it is appended to the main file", async () => {
			// simulate a slow FS
			mockMoveFileThrottle = 50;
			let compressPromise: Promise<void>;
			const map = new Map<any, any>([
				["key1", 1],
				["key2", "2"],
			]);
			for (let i = 1; i < 20; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
					map.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
					map.set(`${i}`, i);
				}
				if (i === 10) compressPromise = db.compress();
				if (i > 10) await wait(20);
			}
			await compressPromise!;

			await db.close();
			await db.open();

			assertEqual(db, map);
		});

		it("blocks the close() call", async () => {
			// simulate a slow FS
			mockMoveFileThrottle = 50;
			const map = new Map<any, any>([
				["key1", 1],
				["key2", "2"],
			]);
			for (let i = 1; i < 20; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
					map.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
					map.set(`${i}`, i);
				}
			}
			db.compress();
			await db.close();
			await db.open();

			assertEqual(db, map);
		});
	});

	describe("compress() regression test: backup file exists", () => {
		const testFilename = "compress-with-bak.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
				[`${testFilename}.bak`]:
					'{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			await testFS.remove();
		});

		it("does not crash", async () => {
			await db.compress();
			await db.close();
		});
	});

	describe("uncompressedSize", () => {
		const testFilename = "uncompressedSize.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: `
{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key1"}
{"k":"key2"}
{"k":"key2","v":"2"}
{"k":"key3","v":3}
{"k":"key3"}
`,
			});
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			await testFS.remove();
			mockMoveFileThrottle = 0;
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			expect(() => db.uncompressedSize).toThrowError("not open");
		});

		it("returns the non-empty line count of the db file", async () => {
			expect(db.uncompressedSize).toBe(7);
		});

		it("increases by 1 for each persisted set command", async () => {
			db.set("key4", 1);
			await wait(50);
			expect(db.uncompressedSize).toBe(8);
			db.set("key4", 1);
			await wait(50);
			expect(db.uncompressedSize).toBe(9);
			db.set("key4", 1);
			await wait(50);
			expect(db.uncompressedSize).toBe(10);
			db.set("key5", 2);
			await wait(50);
			expect(db.uncompressedSize).toBe(11);
		});

		it("increases by 1 for each persisted delete", async () => {
			db.delete("key4");
			await wait(50);
			expect(db.uncompressedSize).toBe(7);
			db.delete("key2");
			await wait(50);
			expect(db.uncompressedSize).toBe(8);
			db.delete("key2");
			await wait(50);
			expect(db.uncompressedSize).toBe(8);
		});

		it("is reset to 0 after clear() is persisted", async () => {
			db.clear();
			await wait(100);
			expect(db.uncompressedSize).toBe(0);
		});

		it("is reset to the compressed size afer compress()", async () => {
			await db.compress();
			expect(db.uncompressedSize).toBe(1);
		});

		it("writes during compress are counted", async () => {
			// simulate a slow FS
			mockMoveFileThrottle = 50;
			const compressPromise = db.compress();
			await wait(20);

			db.set("key1", "value1");
			await compressPromise;

			expect(db.uncompressedSize).toBe(2);
		});
	});

	describe("auto-compression", () => {
		const testFilename = "autoCompress.jsonl";
		let testFilenameFull: string;
		const uncompressed = `{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key3","v":3}
{"k":"key2"}
{"k":"key3","v":3.5}\n`;

		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: `{"k":"key1","v":1}\n`,
				openClose: uncompressed,
			});
		});
		afterEach(async () => {
			await db.close();
			await testFS.remove();
		});

		it("triggers when uncompressedSize >= size * sizeFactor", async () => {
			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					sizeFactor: 4,
				},
			});
			await db.open();

			db.set("key1", 2);
			await wait(25);
			db.set("key1", 3);
			await wait(25);

			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.not.toBe('{"k":"key1","v":3}\n');

			db.set("key1", 4);
			// compress is async, so give it some time
			await wait(100);

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":4}\n',
			);

			await db.close();
		});

		it("..., but only above the minimum size", async () => {
			jest.retryTimes(5);

			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					sizeFactor: 4,
					sizeFactorMinimumSize: 6,
				},
			});
			await db.open();

			for (let i = 2; i <= 5; i++) {
				db.set("key1", i);
				await wait(75);
			}

			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.not.toBe('{"k":"key1","v":5}\n');

			db.set("key1", 6);
			// Wait a bit because compress is async
			await wait(50);
			// close the DB to make sure everything is flushed
			await db.close();

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":6}\n',
			);
		}, 10000);

		it("doesn't trigger when different keys are added", async () => {
			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					sizeFactor: 4,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();

			for (let i = 2; i <= 20; i++) {
				db.set("key" + i, i);
			}
			await db.close();
			expect(compressSpy).not.toBeCalled();
		});

		it("triggers after intervalMs", async () => {
			jest.retryTimes(3); // timeout-based tests are flaky. retry to be sure

			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					intervalMs: 100,
				},
			});
			await db.open();

			db.set("key1", 2);
			await wait(25);
			db.set("key1", 3);
			await wait(25);

			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.not.toBe('{"k":"key1","v":3}\n');

			await wait(75);

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":3}\n',
			);

			await db.close();
		});

		it("..., but only if there were at least intervalMinChanges changes", async () => {
			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					intervalMs: 100,
					intervalMinChanges: 2,
				},
			});
			await db.open();

			db.set("key1", 2);
			await wait(110);
			await expect(
				fs.readFile(testFilenameFull, "utf8"),
			).resolves.not.toBe('{"k":"key1","v":2}\n');

			db.set("key1", 3);
			await wait(110);
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":3}\n',
			);

			await db.close();
		});

		it("compresses after opening when onOpen is true", async () => {
			const testFilenameFull = path.join(testFSRoot, "openClose");
			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					onOpen: true,
				},
			});

			// Cannot use this, since open calls compressInternal
			// expect(compressSpy).toBeCalledTimes(1);
			await db.open();
			await wait(25);
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);

			db.set("key3", 1);
			await db.close();
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n{"k":"key3","v":1}\n',
			);
		});

		it("compresses during close when onClose is true", async () => {
			const testFilenameFull = path.join(testFSRoot, "openClose");
			db = new JsonlDB(testFilenameFull, {
				autoCompress: {
					onClose: true,
				},
			});
			// Cannot use this, since close calls compressInternal
			// expect(compressSpy).toBeCalledTimes(1);
			await db.open();
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				uncompressed,
			);

			await db.close();

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);
		});
	});

	describe("throttling FS", () => {
		const testFilename = "throttled.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create({
				[testFilename]: ``,
			});
		});
		afterEach(async () => {
			if (db) await db.close();
			await testFS.remove();
		});

		async function assertFileContent(content: string): Promise<void> {
			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				content,
			);
		}

		it("changes are only written after intervalMs has passed", async () => {
			jest.retryTimes(3); // timeout-based tests are flaky. retry to be sure

			db = new JsonlDB(testFilenameFull, {
				throttleFS: {
					intervalMs: 100,
				},
			});
			await db.open();
			db.clear();

			// Trigger at least one scheduled cork before the first write
			await wait(110);
			await assertFileContent("");

			db.set("1", 1);

			let expected = `{"k":"1","v":1}\n`;
			await assertFileContent("");

			await wait(50);
			for (let i = 2; i <= 10; i++) {
				db.set(i.toString(), i);
				await assertFileContent("");
				expected += `{"k":"${i}","v":${i}}\n`;
			}

			// Give it a little more time than necessary
			await wait(60);
			await assertFileContent(expected);
		});

		it("or the maximum buffer size was reached", async () => {
			jest.retryTimes(3); // timeout-based tests are flaky. retry to be sure

			db = new JsonlDB(testFilenameFull, {
				throttleFS: {
					intervalMs: 100,
					maxBufferedCommands: 5,
				},
			});
			await db.open();

			db.set("1", 1);
			let expected = `{"k":"1","v":1}\n`;
			await assertFileContent("");

			await wait(50);
			for (let i = 2; i <= 6; i++) {
				db.set(i.toString(), i);
				expected += `{"k":"${i}","v":${i}}\n`;
				if (i <= 5) await assertFileContent("");
			}

			// Give it a little time to write
			await wait(40);
			await assertFileContent(expected);
		});

		it("works after compressing", async () => {
			jest.retryTimes(3); // timeout-based tests are flaky. retry to be sure

			db = new JsonlDB(testFilenameFull, {
				throttleFS: {
					intervalMs: 100,
				},
			});
			await db.open();
			await db.compress();
			await wait(15);

			db.set("1", 1);
			let expected = `{"k":"1","v":1}\n`;
			await assertFileContent("");

			await wait(15);
			for (let i = 2; i <= 100; i++) {
				db.set(i.toString(), i);
				await assertFileContent("");
				expected += `{"k":"${i}","v":${i}}\n`;
			}

			// Give it a little more time than necessary
			await wait(100);
			await assertFileContent(expected);
		});

		it("should still flush the buffer on close", async () => {
			jest.retryTimes(3); // timeout-based tests are flaky. retry to be sure

			db = new JsonlDB(testFilenameFull, {
				throttleFS: {
					intervalMs: 100,
				},
			});
			await db.open();

			db.set("1", 1);
			let expected = `{"k":"1","v":1}\n`;
			await assertFileContent("");

			await wait(50);
			for (let i = 2; i <= 100; i++) {
				db.set(i.toString(), i);
				await assertFileContent("");
				expected += `{"k":"${i}","v":${i}}\n`;
			}

			// Close the db before the next forced flush
			await db.close();

			await assertFileContent(expected);
		});
	});

	describe("consistency checks", () => {
		const testFilename = "checks.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create();
			db = new JsonlDB(testFilenameFull);
			await db.open();
		});
		afterEach(async () => {
			if (db) await db.close();
			await testFS.remove();
		});

		it("opening a complex log restores the same structure as expected", async () => {
			const expected = [
				/* 0: */ { key: "foobar", value: new Date().toISOString() },
				/* 1: */ { key: "asdfasg", value: true },
				/* 2: */ {
					key: 'x"blub',
					value: { lets: "get", complicated: { "!": "?" } },
				},
				/* 3: */ { key: "sssssss", value: null },
				/* 4: */ { key: "1", value: 1 },
				/* 5: */ { key: "2", value: [[[]], {}] },
				/* 6: */ { key: "", value: "what up?!\u0000" },
			];
			db.set(expected[1].key, expected[1].value);
			// 1
			db.set(expected[3].key, expected[3].value);
			// 1, 3
			db.set(expected[2].key, expected[2].value);
			// 1, 3, 2
			db.delete(expected[1].key);
			db.set(expected[5].key, expected[5].value);
			// 3, 2, 5
			db.set(expected[6].key, expected[6].value);
			// 3, 2, 5, 6
			db.set(expected[0].key, expected[0].value);
			// 3, 2, 5, 6, 0
			db.delete(expected[0].key);
			db.set(expected[6].key, expected[6].value);
			db.set(expected[1].key, expected[1].value);
			db.set(expected[4].key, expected[4].value);
			// 3, 2, 5, 6, 1, 4
			db.set(expected[0].key, expected[0].value);
			// 3, 2, 5, 6, 1, 4, 0

			await db.close();
			await db.open();

			expect([...db.keys()].sort()).toEqual(
				expected.map((e) => e.key).sort(),
			);
			expect([...db.values()].sort()).toEqual(
				expected.map((e) => e.value).sort(),
			);
			expect([...db.entries()].sort()).toEqual(
				expected.map((e) => [e.key, e.value]).sort(),
			);
		});
	});

	describe("custom serializer", () => {
		const testFilename = "serializer.jsonl";
		let testFilenameFull: string;
		let db: JsonlDB;
		let testFS: TestFS;
		let testFSRoot: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
			await testFS.create();
		});
		afterEach(async () => {
			await testFS.remove();
		});

		it("before saving, values are transformed using the serializer function if any is passed", async () => {
			const serializer = jest.fn().mockReturnValue("ffff");
			db = new JsonlDB(testFilenameFull, { serializer });
			await db.open();

			const map = new Map<any, any>([
				[1, "foo"],
				[2, "bar"],
			]);
			db.set("test", map);
			await db.close();

			await wait(10);

			await expect(fs.readFile(testFilenameFull, "utf8")).resolves.toBe(
				`{"k":"test","v":"ffff"}\n`,
			);
		});
	});

	describe("crash recovery", () => {
		let testFS: TestFS;
		let testFSRoot: string;
		let db: JsonlDB;
		const testFilename = "recovery.jsonl";
		let testFilenameFull: string;

		beforeEach(async () => {
			testFS = new TestFS();
			testFSRoot = await testFS.getRoot();
			testFilenameFull = path.join(testFSRoot, testFilename);
		});

		afterEach(async () => {
			if (db) await db.close();
			await testFS.remove();
		});

		async function assertCleanedUp() {
			// The other files should have been cleaned up
			await expect(fs.pathExists(testFilenameFull)).resolves.toBeTrue();
			await expect(
				fs.pathExists(testFilenameFull + ".bak"),
			).resolves.toBeFalse();
			await expect(
				fs.pathExists(testFilenameFull + ".dump"),
			).resolves.toBeFalse();
		}

		it("db truncated, .bak ok -> use .bak", async () => {
			await testFS.create({
				// Original, uncompressed db in the .bak file
				[testFilename + ".bak"]: `
{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key3","v":3}
{"k":"key2"}
{"k":"key3","v":3.5}`,
				// empty, broken db file
				[testFilename]: "",
				// (probably) half-complete .dump file
				[testFilename + ".dump"]: `
{"k":"key1","v":1}
{"k":"key3","v":3}`,
			});

			const db = new JsonlDB(testFilenameFull);
			await db.open();

			expect(db.size).toBe(2);
			expect(db.has("key1")).toBeTrue();
			expect(db.has("key2")).toBeFalse();
			expect(db.has("key3")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.get("key3")).toBe(3.5);

			await assertCleanedUp();
			await db.close();
		});

		it("db missing, .bak ok -> use .bak", async () => {
			await testFS.create({
				// Original, uncompressed db in the .bak file
				[testFilename + ".bak"]: `
{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key3","v":3}
{"k":"key2"}
{"k":"key3","v":3.5}`,
				// (probably) half-complete .dump file
				[testFilename + ".dump"]: `
{"k":"key1","v":1}
{"k":"key3","v":3}`,
			});

			const db = new JsonlDB(testFilenameFull);
			await db.open();

			expect(db.size).toBe(2);
			expect(db.has("key1")).toBeTrue();
			expect(db.has("key2")).toBeFalse();
			expect(db.has("key3")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.get("key3")).toBe(3.5);

			await assertCleanedUp();

			await db.close();
		});

		it("db truncated, .bak truncated, .dump ok -> use .dump", async () => {
			await testFS.create({
				// empty, broken .bak file
				[testFilename + ".bak"]: "",
				// empty, broken db file
				[testFilename]: "",
				// (probably) half-complete .dump file, but better than nothing
				[testFilename + ".dump"]: `
{"k":"key1","v":1}
{"k":"key3","v":3}`,
			});

			const db = new JsonlDB(testFilenameFull);
			await db.open();

			expect(db.size).toBe(2);
			expect(db.has("key1")).toBeTrue();
			expect(db.has("key2")).toBeFalse();
			expect(db.has("key3")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.get("key3")).toBe(3);

			await assertCleanedUp();

			await db.close();
		});

		it("db truncated, .bak missing, .dump ok -> use .dump", async () => {
			await testFS.create({
				// empty, broken db file
				[testFilename]: "",
				// (probably) half-complete .dump file, but better than nothing
				[testFilename + ".dump"]: `
{"k":"key1","v":1}
{"k":"key3","v":3}`,
			});

			const db = new JsonlDB(testFilenameFull);
			await db.open();

			expect(db.size).toBe(2);
			expect(db.has("key1")).toBeTrue();
			expect(db.has("key2")).toBeFalse();
			expect(db.has("key3")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.get("key3")).toBe(3);

			await assertCleanedUp();

			await db.close();
		});
	});
});
