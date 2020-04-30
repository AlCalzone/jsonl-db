import { wait } from "alcalzone-shared/async";
import * as fs from "fs-extra";
import mockFs from "mock-fs";
import { JsonlDB } from "./db";

let mockAppendFileThrottle = 0;
let mockMoveFileThrottle = 0;

jest.mock("fs-extra", () => {
	const originalFS = jest.requireActual("fs-extra");
	const wait = require("alcalzone-shared/async").wait;
	return {
		__esModule: true, // Use it when dealing with esModules
		...originalFS,
		appendFile: jest.fn().mockImplementation(async (fs, str) => {
			if (mockAppendFileThrottle > 0) {
				await wait(mockAppendFileThrottle);
			}
			return originalFS.appendFile(fs, str);
		}),
		move: jest.fn().mockImplementation(async (src, dest) => {
			if (mockMoveFileThrottle > 0) {
				await wait(mockMoveFileThrottle);
			}
			return originalFS.move(src, dest);
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
	}
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
	});

	describe("open()", () => {
		beforeEach(() => {
			mockFs({
				yes:
					// Final newline omitted on purpose
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}',
				emptyLines:
					'\n{"k": "key1", "v": 1}\n\n\n{"k": "key2", "v": "2"}\n\n',
				broken: `{"k": "key1", "v": 1}\n{"k":,"v":1}\n`,
				reviver: `
{"k": "key1", "v": 1}
{"k": "key2", "v": "2"}
{"k": "key1"}
{"k": "key1", "v": true}`,
			});
		});
		afterEach(mockFs.restore);

		it("sets the isOpen property to true", async () => {
			const db = new JsonlDB("yes");
			await db.open();
			expect(db.isOpen).toBeTrue();
		});

		it("checks if the given file exists and creates it if it doesn't", async () => {
			const db = new JsonlDB("no");
			await db.open();
			await db.close();
		});

		it("reads the file if it exists", async () => {
			const db = new JsonlDB("yes");
			await db.open();
			await db.close();
		});

		it("should contain the correct data", async () => {
			const db = new JsonlDB("yes");
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
			const db = new JsonlDB("emptyLines");
			await db.open();

			expect(db.has("key1")).toBeTrue();
			expect(db.get("key1")).toBe(1);
			expect(db.has("key2")).toBeTrue();
			expect(db.get("key2")).toBe("2");

			await db.close();
		});

		it("throws when the file contains invalid JSON", async () => {
			const db = new JsonlDB("broken");
			try {
				await db.open();
				throw new Error("it did not throw");
			} catch (e) {
				expect(e.message).toMatch(/invalid data/i);
				expect(e.message).toMatch("line 2");
			}
		});

		it("does not throw when the file contains invalid JSON and `ignoreReadErrors` is true", async () => {
			const db = new JsonlDB("broken", { ignoreReadErrors: true });
			await expect(db.open()).toResolve();
		});

		it("transforms each value using the valueReviver function if any is passed", async () => {
			const reviver = jest.fn().mockReturnValue("eeee");
			const db = new JsonlDB("reviver", { reviver });
			await db.open();
			expect(reviver).toBeCalledTimes(3);
			expect(reviver).toBeCalledWith("key1", 1);
			expect(reviver).toBeCalledWith("key2", "2");
			expect(reviver).toBeCalledWith("key1", true);

			db.forEach((v) => {
				expect(v).toBe("eeee");
			});
		});
	});

	describe("clear()", () => {
		const testFilename = "clear.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]:
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}\n',
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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

			await expect(fs.stat(testFilename)).resolves.toMatchObject({
				size: 0,
			});
		});
	});

	describe("delete()", () => {
		const testFilename = "delete.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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

			await expect(fs.readFile(testFilename, "utf8")).resolves.toEndWith(
				`{"k":"key2"}\n`,
			);
		});

		it("removes multiple key from the database in the correct order", async () => {
			expect(db.delete("key2")).toBeTrue();
			expect(db.delete("key1")).toBeTrue();
			expect(db.size).toBe(0);
			expect(db.has("key1")).toBeFalse();
			expect(db.has("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilename, "utf8")).resolves.toEndWith(
				`{"k":"key2"}\n{"k":"key1"}\n`,
			);
		});

		it("deleting a key twice does not write to the log twice", async () => {
			expect(db.delete("key2")).toBeTrue();
			expect(db.delete("key2")).toBeFalse();

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				`{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n{"k":"key2"}\n`,
			);
		});
	});

	describe("set()", () => {
		const testFilename = "set.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs();
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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

			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				`{"k":"key","v":true}\n`,
			);
		});

		it("adds multiple keys to the database in the correct order", async () => {
			db.set("key2", true);
			db.set("key1", 1000);
			db.set("key3", "");

			// Force the stream to be flushed
			await db.close();

			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				`{"k":"key2","v":true}\n{"k":"key1","v":1000}\n{"k":"key3","v":""}\n`,
			);
		});
	});

	describe("importJson()", () => {
		const testFilename = "import.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
				jsonFile: '{"key3": 1, "key4": true}',
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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
			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				`{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"1","v":1}
{"k":"foo","v":"bar"}
{"k":"baz","v":"inga"}
`,
			);
		});

		it("the file version asynchronously adds all keys and values to the database", async () => {
			await db.importJson("jsonFile");
			// Force the stream to be flushed
			await db.close();

			// The order changes because Object.entries reads the entries in a different order
			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
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
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
				jsonFile: '{"key3": 1, "key4": true}',
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

		it("throws when the DB is not open", async () => {
			await db.close();
			await expect(db.exportJson("jsonFile")).toReject();
		});

		it("overwrites the given file with the DB contents as valid JSON", async () => {
			await db.exportJson("jsonfile");
			await expect(fs.readFile("jsonfile", "utf8")).resolves.toBe(
				`{"key1":1,"key2":"2"}\n`,
			);
		});

		it("honors the JSON formatting options", async () => {
			await db.exportJson("jsonfile", { spaces: "\t" });
			await expect(fs.readFile("jsonfile", "utf8")).resolves.toBe(
				`{
	"key1": 1,
	"key2": "2"
}\n`,
			);
		});
	});

	describe("close()", () => {
		const testFilename = "close.jsonl";
		// The basic functionality is tested in the other suites
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs();
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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
		let db: JsonlDB;
		let dumpdb: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: "",
				[testFilename + ".dump"]: "",
			});
			db = new JsonlDB(testFilename);
			dumpdb = new JsonlDB(testFilename + ".dump");
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			await dumpdb.close();
			mockFs.restore();
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
			for (let i = 1; i < 20; i++) {
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
			await wait(100);
			// and write something that will be put into the dump backlog
			db.set("21", 21);

			await db.close();
			await db.open();
			await dumpdb.open();
			assertEqual(db, dumpdb);
		});
	});

	describe("compress()", () => {
		const testFilename = "compress.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			mockFs.restore();
			mockMoveFileThrottle = 0;
			mockAppendFileThrottle = 0;
		});

		it("overwrites the append-only file with a compressed version", async () => {
			db.set("key3", 3);
			db.delete("key2");
			db.set("key3", 3.5);

			await db.compress();
			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);
			await expect(
				fs.pathExists(testFilename + ".dump"),
			).resolves.toBeFalse();
			await expect(
				fs.pathExists(testFilename + ".bak"),
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
			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n{"k":"key2","v":1}\n',
			);
		});

		it("does not do anything while the DB is being closed", async () => {
			db.set("key3", 3);
			db.delete("key2");
			db.set("key3", 3.5);
			const closePromise = db.close();
			await db.compress();
			await closePromise;

			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
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

			await expect(fs.readFile(testFilename, "utf8")).resolves.toBe(
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

	describe("uncompressedSize", () => {
		const testFilename = "uncompressedSize.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: `
{"k": "key1", "v": 1}
{"k": "key2", "v": "2"}
{"k": "key1"}
{"k": "key2"}
{"k": "key2", "v": "2"}
{"k": "key3", "v": 3}
{"k": "key3"}
`,
			});
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			mockFs.restore();
			mockMoveFileThrottle = 0;
		});

		it("throws when the DB is not open", async () => {
			await db.close();
			expect(() => db.uncompressedSize).toThrowError("not open");
		});

		it("returns the non-empty line count of the db file", async () => {
			expect(db.uncompressedSize).toBe(7);
		});

		it("increases by 1 for each set command", async () => {
			db.set("key4", 1);
			expect(db.uncompressedSize).toBe(8);
			db.set("key4", 1);
			expect(db.uncompressedSize).toBe(9);
			db.set("key4", 1);
			expect(db.uncompressedSize).toBe(10);
			db.set("key5", 2);
			expect(db.uncompressedSize).toBe(11);
		});

		it("increases by 1 for each non-noop delete", async () => {
			db.delete("key4");
			expect(db.uncompressedSize).toBe(7);
			db.delete("key2");
			expect(db.uncompressedSize).toBe(8);
			db.delete("key2");
			expect(db.uncompressedSize).toBe(8);
		});

		it("is reset to 0 after clear()", async () => {
			db.clear();
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
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: `{"k": "key1", "v": 1}`,
				openClose: `
{"k":"key1","v":1}
{"k":"key2","v":"2"}
{"k":"key3","v":3}
{"k":"key2"}
{"k":"key3","v":3.5}`,
			});
		});
		afterEach(async () => {
			await db.close();
			mockFs.restore();
		});

		it("triggers when uncompressedSize >= size * sizeFactor", async () => {
			db = new JsonlDB(testFilename, {
				autoCompress: {
					sizeFactor: 4,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();

			for (let i = 2; i <= 9; i++) {
				db.set("key1", i);
				// compress is async, so give it some time
				await wait(20);
				if (i <= 3) {
					expect(compressSpy).not.toBeCalled();
				} else if (i <= 6) {
					expect(compressSpy).toBeCalledTimes(1);
				} else {
					expect(compressSpy).toBeCalledTimes(2);
				}
			}

			await db.close();
		});

		it("..., but only above the minimum size", async () => {
			db = new JsonlDB(testFilename, {
				autoCompress: {
					sizeFactor: 4,
					sizeFactorMinimumSize: 20,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();

			for (let i = 2; i <= 20; i++) {
				db.set("key1", i);
				// compress is async, so give it some time
				await wait(20);
			}
			await db.close();
			expect(compressSpy).toBeCalledTimes(1);
		});

		it("doesn't trigger when different keys are added", async () => {
			db = new JsonlDB(testFilename, {
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
			db = new JsonlDB(testFilename, {
				autoCompress: {
					intervalMs: 100,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();

			for (let i = 2; i <= 15; i++) {
				db.set("key1", i);
				// compress is async, so give it some time
				await wait(20);
				if (i <= 5) {
					expect(compressSpy).not.toBeCalled();
				} else if (i <= 10) {
					expect(compressSpy).toBeCalledTimes(1);
				} else {
					expect(compressSpy).toBeCalledTimes(2);
				}
			}

			await db.close();
		});

		it("..., but only if there were at least intervalMinChanges changes", async () => {
			db = new JsonlDB(testFilename, {
				autoCompress: {
					intervalMs: 100,
					intervalMinChanges: 2,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();

			await wait(100);
			expect(compressSpy).not.toBeCalled();

			db.set("key1", 1);
			await wait(100);
			expect(compressSpy).not.toBeCalled(); // only 1 change

			db.set("key1", 1);
			await wait(100);
			expect(compressSpy).toBeCalledTimes(1); // two changes

			await db.close();
		});

		it("compresses after opening when onOpen is true", async () => {
			db = new JsonlDB("openClose", {
				autoCompress: {
					onOpen: true,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();
			expect(compressSpy).toBeCalledTimes(1);

			await wait(20);

			await expect(fs.readFile("openClose", "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);

			await db.close();
			expect(compressSpy).toBeCalledTimes(1);
		});

		it("compresses during close when onClose is true", async () => {
			db = new JsonlDB("openClose", {
				autoCompress: {
					onClose: true,
				},
			});
			const compressSpy = jest.spyOn(db, "compress");
			await db.open();
			expect(compressSpy).not.toBeCalled();
			await db.close();
			// Cannot use this, since close calls compressInternal
			// expect(compressSpy).toBeCalledTimes(1);

			await expect(fs.readFile("openClose", "utf8")).resolves.toBe(
				'{"k":"key1","v":1}\n{"k":"key3","v":3.5}\n',
			);
		});
	});

	describe("consistency checks", () => {
		const testFilename = "checks.jsonl";
		let db: JsonlDB;
		beforeEach(async () => {
			mockFs();
			db = new JsonlDB(testFilename);
			await db.open();
		});
		afterEach(mockFs.restore);

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
});
