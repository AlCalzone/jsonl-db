import {
	createDeferredPromise,
	DeferredPromise,
} from "alcalzone-shared/deferred-promise";
import * as fs from "fs-extra";
import * as readline from "readline";
import * as stream from "stream";

export class DB<V extends unknown = unknown> {
	public constructor(public readonly filename: string) {
		// Bind all map properties we can use directly
		this.forEach = this._db.forEach.bind(this._db);
		this.get = this._db.get.bind(this._db);
		this.has = this._db.has.bind(this._db);
		this.entries = this._db.entries.bind(this._db);
		this.keys = this._db.keys.bind(this._db);
		this.values = this._db.values.bind(this._db);
		this[Symbol.iterator] = this._db[Symbol.iterator].bind(this._db);
	}

	private _db = new Map<string, V>();
	// Declare all map properties we can use directly
	declare forEach: Map<string, V>["forEach"];
	declare get: Map<string, V>["get"];
	declare has: Map<string, V>["has"];
	declare [Symbol.iterator]: () => IterableIterator<[string, V]>;
	declare entries: Map<string, V>["entries"];
	declare keys: Map<string, V>["keys"];
	declare values: Map<string, V>["values"];

	public get size(): number {
		return this._db.size;
	}

	private _fd: number | undefined;
	private _writeLog: stream.PassThrough | undefined;

	/** Opens a file and allows iterating over all lines */
	private readLines(): stream.Readable {
		const output = new stream.PassThrough({ objectMode: true });
		const readStream = fs.createReadStream(this.filename, "utf8");
		const rl = readline.createInterface(readStream);
		rl.on("line", (line) => {
			output.write(line);
		});
		rl.on("close", () => {
			output.end();
		});
		return output;
	}

	/** Opens the database file or creates it if it doesn't exist */
	public async open(): Promise<void> {
		// Open the file for appending and reading
		this._fd = await fs.open(this.filename, "a+");
		for await (const line of this.readLines()) {
			this.parseLine(line);
		}
		// Start the write thread
		this.writeThread();
	}

	/** Parses a line and updates the internal DB correspondingly */
	private parseLine(line: string): void {
		const record: { k: string; v?: V } = JSON.parse(line);
		const { k, v } = record;
		if (v !== undefined) {
			this._db.set(k, v);
		} else {
			this._db.delete(k);
		}
	}

	public clear(): void {
		this._db.clear();
		this._writeLog!.write("");
	}
	public delete(key: string): boolean {
		const ret = this._db.delete(key);
		if (ret) {
			// Something was deleted
			this._writeLog!.write(JSON.stringify({ k: key }));
		}
		return ret;
	}
	public set(key: string, value: V): this {
		this._db.set(key, value);
		this._writeLog!.write(JSON.stringify({ k: key, v: value }));
		return this;
	}

	/** Asynchronously performs all write actions */
	private async writeThread(): Promise<void> {
		// TODO: use cork() and uncork()
		this._writeLog = new stream.PassThrough({ objectMode: true });
		for await (const action of this._writeLog as AsyncIterable<string>) {
			if (action === "") {
				await fs.ftruncate(this._fd!);
			} else {
				await fs.write(this._fd!, action + "\n");
			}
		}
		// The write log was closed, this means that the DB is being closed
		// close the file and resolve the close promise
		await fs.close(this._fd!);
		this._closePromise?.resolve();
	}

	private _closePromise: DeferredPromise<void> | undefined;
	/** Closes the DB and waits for all data to be written */
	public async close(): Promise<void> {
		if (this._writeLog) {
			this._closePromise = createDeferredPromise();
			this._writeLog.end();
			await this._closePromise;
		}
		// Reset all variables
		this._closePromise = undefined;
		this._db.clear();
		this._fd = undefined;
		this._writeLog = undefined;
	}
}
