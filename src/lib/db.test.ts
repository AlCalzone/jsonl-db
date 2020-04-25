import { wait } from "alcalzone-shared/async";
import * as fs from "fs-extra";
import mockFs from "mock-fs";
import { DB } from "./db";

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
	describe("open()", () => {
		beforeEach(() => {
			mockFs({
				yes:
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}\n',
			});
		});
		afterEach(mockFs.restore);

		it("sets the isOpen property to true", async () => {
			const db = new DB("yes");
			await db.open();
			expect(db.isOpen).toBeTrue();
		});

		it("checks if the given file exists and creates it if it doesn't", async () => {
			const db = new DB("no");
			await db.open();
			await db.close();
		});

		it("reads the file if it exists", async () => {
			const db = new DB("yes");
			await db.open();
			await db.close();
		});

		it("should contain the correct data", async () => {
			const db = new DB("yes");
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
	});

	describe("clear()", () => {
		const testFilename = "clear.jsonl";
		let db: DB;
		beforeEach(async () => {
			mockFs({
				[testFilename]:
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}\n',
			});
			db = new DB(testFilename);
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
		let db: DB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new DB(testFilename);
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
		let db: DB;
		beforeEach(async () => {
			mockFs();
			db = new DB(testFilename);
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

	describe("close()", () => {
		const testFilename = "close.jsonl";
		// The basic functionality is tested in the other suites
		let db: DB;
		beforeEach(async () => {
			mockFs();
			db = new DB(testFilename);
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
		let db: DB;
		let dumpdb: DB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: "",
				[testFilename + ".dump"]: "",
			});
			db = new DB(testFilename);
			dumpdb = new DB(testFilename + ".dump");
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
	});

	describe("compress()", () => {
		const testFilename = "compress.jsonl";
		let db: DB;
		beforeEach(async () => {
			mockFs({
				[testFilename]: '{"k":"key1","v":1}\n{"k":"key2","v":"2"}\n',
			});
			db = new DB(testFilename);
			await db.open();
		});
		afterEach(async () => {
			await db.close();
			mockFs.restore();
			mockMoveFileThrottle = 0;
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

		it("does not do anything when the DB is being closed", async () => {
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
	});

	describe("consistency checks", () => {
		const testFilename = "checks.jsonl";
		let db: DB;
		beforeEach(async () => {
			mockFs();
			db = new DB(testFilename);
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
