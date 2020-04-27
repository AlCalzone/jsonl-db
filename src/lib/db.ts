import {
	createDeferredPromise,
	DeferredPromise,
} from "alcalzone-shared/deferred-promise";
import { composeObject } from "alcalzone-shared/objects";
import * as fs from "fs-extra";
import * as readline from "readline";
import * as stream from "stream";

export class JsonlDB<V extends unknown = unknown> {
	public constructor(filename: string) {
		this.filename = filename;
		this.dumpFilename = this.filename + ".dump";
		// Bind all map properties we can use directly
		this.forEach = this._db.forEach.bind(this._db);
		this.get = this._db.get.bind(this._db);
		this.has = this._db.has.bind(this._db);
		this.entries = this._db.entries.bind(this._db);
		this.keys = this._db.keys.bind(this._db);
		this.values = this._db.values.bind(this._db);
		this[Symbol.iterator] = this._db[Symbol.iterator].bind(this._db);
	}

	public readonly filename: string;
	public readonly dumpFilename: string;

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

	private _isOpen: boolean = false;
	public get isOpen(): boolean {
		return this._isOpen;
	}
	private _fd: number | undefined;
	private _dumpFd: number | undefined;
	private _compressBacklog: stream.PassThrough | undefined;
	private _writeBacklog: stream.PassThrough | undefined;
	private _dumpBacklog: stream.PassThrough | undefined;

	/** Opens a file and allows iterating over all lines */
	private readLines(): stream.Readable {
		const output = new stream.PassThrough({ objectMode: true });
		const readStream = fs.createReadStream(this.filename, {
			encoding: "utf8",
			fd: this._fd!,
			autoClose: false,
		});
		const rl = readline.createInterface(readStream);
		rl.on("line", (line) => {
			output.write(line);
		});
		rl.on("close", () => {
			output.end();
		});
		return output;
	}

	private _openPromise: DeferredPromise<void> | undefined;
	// /** Opens the database file or creates it if it doesn't exist */
	public async open(): Promise<void> {
		// Open the file for appending and reading
		this._fd = await fs.open(this.filename, "a+");
		for await (const line of this.readLines()) {
			this.parseLine(line);
		}
		// Close the file again to avoid EBADF
		await fs.close(this._fd);
		this._fd = undefined;
		// Start the write thread
		this._openPromise = createDeferredPromise();
		void this.writeThread();
		await this._openPromise;
		this._isOpen = true;
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
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		this._db.clear();
		this.write("");
	}
	public delete(key: string): boolean {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		const ret = this._db.delete(key);
		if (ret) {
			// Something was deleted
			this.write(this.entryToLine(key));
		}
		return ret;
	}
	public set(key: string, value: V): this {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		this._db.set(key, value);
		this.write(this.entryToLine(key, value));
		return this;
	}

	private async importJsonFile(filename: string): Promise<void> {
		const json = await fs.readJSON(filename);
		return this.importJson(json);
	}

	public importJson(filename: string): Promise<void>;
	public importJson(json: Record<string, any>): void;
	public importJson(
		jsonOrFile: Record<string, any> | string,
	): void | Promise<void> {
		if (typeof jsonOrFile === "string") {
			if (!this._isOpen) {
				return Promise.reject(new Error("The database is not open!"));
			}
			return this.importJsonFile(jsonOrFile);
		} else {
			if (!this._isOpen) {
				throw new Error("The database is not open!");
			}
		}

		for (const [key, value] of Object.entries(jsonOrFile)) {
			this._db.set(key, value);
			this.write(this.entryToLine(key, value));
		}
	}

	public async exportJson(
		filename: string,
		options?: fs.WriteOptions,
	): Promise<void> {
		if (!this._isOpen) {
			return Promise.reject(new Error("The database is not open!"));
		}
		return fs.writeJSON(filename, composeObject([...this._db]), options);
	}

	// TODO: use cork() and uncork() to throttle filesystem accesses

	private write(line: string): void {
		/* istanbul ignore else */
		if (this._compressBacklog && !this._compressBacklog.destroyed) {
			this._compressBacklog.write(line);
		} else if (this._writeBacklog && !this._writeBacklog.destroyed) {
			this._writeBacklog.write(line);
		} else {
			throw new Error(
				"Cannot write into the database while no streams are open!",
			);
		}
		// If necessary, write to the dump backlog, so the dump doesn't miss any data
		if (this._dumpBacklog && !this._dumpBacklog.destroyed) {
			this._dumpBacklog.write(line);
		}
	}

	private entryToLine(key: string, value?: V): string {
		if (value !== undefined) {
			return JSON.stringify({ k: key, v: value });
		} else {
			return JSON.stringify({ k: key });
		}
	}

	/** Saves a compressed copy of the DB into `<filename>.dump` */
	public async dump(): Promise<void> {
		this._closeDumpPromise = createDeferredPromise();
		// Open the file for writing (or truncate if it exists)
		this._dumpFd = await fs.open(this.dumpFilename, "w+");
		// And start dumping the DB
		// Start by creating a dump backlog, so parallel writes will be remembered
		this._dumpBacklog = new stream.PassThrough({ objectMode: true });
		// Create a copy of the other entries in the DB
		const entries = [...this._db];
		// And persist them
		for (const [key, value] of entries) {
			await fs.appendFile(
				this._dumpFd,
				this.entryToLine(key, value) + "\n",
			);
		}
		// In case there is any data in the backlog stream, persist that too
		let line: string;
		while (null !== (line = this._dumpBacklog.read())) {
			await fs.appendFile(this._dumpFd, line + "\n");
		}
		this._dumpBacklog.destroy();
		this._dumpBacklog = undefined;

		// The dump backlog was closed, this means that the dump is finished.
		// Close the file and resolve the close promise
		await fs.close(this._dumpFd);

		this._dumpFd = undefined;
		this._closeDumpPromise.resolve();
	}

	/** Asynchronously performs all write actions */
	private async writeThread(): Promise<void> {
		// This must be called before any awaits
		this._writeBacklog = new stream.PassThrough({ objectMode: true });
		// Open the file for appending and reading
		this._fd = await fs.open(this.filename, "a+");
		this._openPromise?.resolve();
		for await (const action of this._writeBacklog as AsyncIterable<
			string
		>) {
			if (action === "") {
				// Since we opened the file in append mode, we cannot truncate
				// therefore close and open in write mode again
				await fs.close(this._fd);
				this._fd = await fs.open(this.filename, "w+");
			} else {
				await fs.appendFile(this._fd, action + "\n");
			}
		}
		// The write backlog was closed, this means that the DB is being closed
		// close the file and resolve the close promise
		await fs.close(this._fd);
		this._closeDBPromise?.resolve();
	}

	/** Compresses the db by dumping it and overwriting the aof file. */
	public async compress(): Promise<void> {
		if (!this._writeBacklog) return;
		await this.dump();
		// After dumping, restart the write thread so no duplicate entries get written
		this._closeDBPromise = createDeferredPromise();
		// Disable writing into the backlog stream and buffer all writes
		// in the compress backlog in the meantime
		this._compressBacklog = new stream.PassThrough({ objectMode: true });
		this._writeBacklog.end();
		this._writeBacklog = undefined;
		await this._closeDBPromise;

		// Replace the aof file
		await fs.move(this.filename, this.filename + ".bak");
		await fs.move(this.dumpFilename, this.filename);
		await fs.unlink(this.filename + ".bak");

		// Start the write thread again
		this._openPromise = createDeferredPromise();
		void this.writeThread();
		await this._openPromise;

		// In case there is any data in the backlog stream, persist that too
		let line: string;
		while (null !== (line = this._compressBacklog.read())) {
			this._writeBacklog!.write(line);
		}
		this._compressBacklog.destroy();
		this._compressBacklog = undefined;
	}

	private _closeDBPromise: DeferredPromise<void> | undefined;
	private _closeDumpPromise: DeferredPromise<void> | undefined;
	/** Closes the DB and waits for all data to be written */
	public async close(): Promise<void> {
		this._isOpen = false;
		if (this._writeBacklog) {
			this._closeDBPromise = createDeferredPromise();
			// Disable writing into the backlog stream
			this._writeBacklog.end();
			this._writeBacklog = undefined;
			// Disable writing into the dump backlog stream
			this._dumpBacklog?.end();
			this._dumpBacklog = undefined;
			await this._closeDBPromise;
		}

		// Also wait for a potential dump process to finish
		if (this._closeDumpPromise) {
			await this._closeDumpPromise;
		}

		// Reset all variables
		this._closeDBPromise = undefined;
		this._closeDumpPromise = undefined;
		this._db.clear();
		this._fd = undefined;
		this._dumpFd = undefined;
	}
}
