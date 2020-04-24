import * as fs from "fs-extra";
import mockFs from "mock-fs";
import { DB } from "./db";

jest.mock("fs-extra", () => {
	const originalFS = jest.requireActual("fs-extra");
	return {
		__esModule: true, // Use it when dealing with esModules
		...originalFS,
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

describe("lib/db", () => {
	describe("open()", () => {
		beforeEach(() => {
			mockFs({
				yes:
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}\n',
			});
		});
		afterEach(mockFs.restore);

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
		beforeAll(async () => {
			mockFs({
				[testFilename]:
					'{"k": "key1", "v": 1}\n{"k": "key2", "v": "2"}\n{"k": "key1"}\n',
			});
			db = new DB(testFilename);
			await db.open();
		});
		afterAll(mockFs.restore);

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
		});
		afterEach(async () => {
			mockFs.restore();
		});

		it("writes a compressed version of the database", async () => {
			await db.open();
			for (let i = 1; i < 20; i++) {
				if (i % 4 === 0) {
					db.delete(`${i - 1}`);
				} else {
					db.set(`${i}`, i);
				}
			}
			await db.dump();

			await db.close();
			await dumpdb.close();

			await dumpdb.open();

			for (const key of db.keys()) {
				expect(dumpdb.has(key)).toBeTrue();
				expect(dumpdb.get(key)).toBe(db.get(key));
			}
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
