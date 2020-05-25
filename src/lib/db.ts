import {
	createDeferredPromise,
	DeferredPromise,
} from "alcalzone-shared/deferred-promise";
import { composeObject } from "alcalzone-shared/objects";
import * as fs from "fs-extra";
import * as path from "path";
import * as readline from "readline";
import * as stream from "stream";

export interface JsonlDBOptions<V> {
	/**
	 * Whether errors reading the db file (e.g. invalid JSON) should silently be ignored.
	 * **Warning:** This may result in inconsistent data!
	 */
	ignoreReadErrors?: boolean;
	/**
	 * An optional reviver function (similar to JSON.parse) to transform parsed values before they are accessible in the database.
	 * If this function is defined, it must always return a value.
	 */
	reviver?: (key: string, value: any) => V;
	/**
	 * An optional serializer function (similar to JSON.serialize) to transform values before they are written to the database file.
	 * If this function is defined, it must always return a value.
	 */
	serializer?: (key: string, value: V) => any;

	/**
	 * Configure when the DB should be automatically compressed.
	 * If multiple conditions are configured, the DB is compressed when any of them are fulfilled
	 */
	autoCompress?: Partial<{
		/**
		 * Compress when uncompressedSize >= size * sizeFactor. Default: +Infinity
		 */
		sizeFactor: number;
		/**
		 * Configure the minimum size necessary for auto-compression based on size. Default: 0
		 */
		sizeFactorMinimumSize: number;
		/**
		 * Compress after a certain time has passed. Default: never
		 */
		intervalMs: number;
		/**
		 * Configure the minimum count of changes for auto-compression based on time. Default: 1
		 */
		intervalMinChanges: number;
		/** Compress when closing the DB. Default: false */
		onClose: boolean;
		/** Compress after opening the DB. Default: false */
		onOpen: boolean;
	}>;

	/**
	 * Can be used to throttle write accesses to the filesystem. By default,
	 * every change is immediately written to the FS
	 */
	throttleFS?: {
		/**
		 * Minimum wait time between two consecutive write accesses. Default: 0
		 */
		intervalMs: number;
		/**
		 * Maximum commands to be buffered before forcing a write access. Default: +Infinity
		 */
		maxBufferedCommands?: number;
	};
}

export class JsonlDB<V extends unknown = unknown> {
	public constructor(filename: string, options: JsonlDBOptions<V> = {}) {
		this.validateOptions(options);

		this.filename = filename;
		this.dumpFilename = this.filename + ".dump";
		this.options = options;
		// Bind all map properties we can use directly
		this.forEach = this._db.forEach.bind(this._db);
		this.get = this._db.get.bind(this._db);
		this.has = this._db.has.bind(this._db);
		this.entries = this._db.entries.bind(this._db);
		this.keys = this._db.keys.bind(this._db);
		this.values = this._db.values.bind(this._db);
		this[Symbol.iterator] = this._db[Symbol.iterator].bind(this._db);
	}

	private validateOptions(options: JsonlDBOptions<V>): void {
		if (options.autoCompress) {
			const {
				sizeFactor,
				sizeFactorMinimumSize,
				intervalMs,
				intervalMinChanges,
			} = options.autoCompress;
			if (sizeFactor != undefined && sizeFactor <= 1) {
				throw new Error("sizeFactor must be > 1");
			}
			if (
				sizeFactorMinimumSize != undefined &&
				sizeFactorMinimumSize < 0
			) {
				throw new Error("sizeFactorMinimumSize must be >= 0");
			}
			if (intervalMs != undefined && intervalMs < 10) {
				throw new Error("intervalMs must be >= 10");
			}
			if (intervalMinChanges != undefined && intervalMinChanges < 1) {
				throw new Error("intervalMinChanges must be >= 1");
			}
		}
		if (options.throttleFS) {
			const { intervalMs, maxBufferedCommands } = options.throttleFS;
			if (intervalMs < 0) {
				throw new Error("intervalMs must be >= 0");
			}
			if (maxBufferedCommands != undefined && maxBufferedCommands < 0) {
				throw new Error("maxBufferedCommands must be >= 0");
			}
		}
	}

	public readonly filename: string;
	public readonly dumpFilename: string;

	private options: JsonlDBOptions<V>;

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

	private _uncompressedSize: number = Number.NaN;
	/** Returns the line count of the appendonly file, excluding empty lines */
	public get uncompressedSize(): number {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		return this._uncompressedSize;
	}

	private _changesSinceLastCompress: number = 0;

	private _isOpen: boolean = false;
	public get isOpen(): boolean {
		return this._isOpen;
	}
	private _fd: number | undefined;
	private _dumpFd: number | undefined;
	private _compressBacklog: stream.PassThrough | undefined;
	private _writeBacklog: stream.PassThrough | undefined;
	private _writeCorkCount = 0;
	private _writeCorkTimeout: NodeJS.Timeout | undefined;
	private _dumpBacklog: stream.PassThrough | undefined;
	private _compressInterval: NodeJS.Timeout | undefined;

	private _openPromise: DeferredPromise<void> | undefined;
	// /** Opens the database file or creates it if it doesn't exist */
	public async open(): Promise<void> {
		// Open the file for appending and reading
		await fs.ensureDir(path.dirname(this.filename));
		this._fd = await fs.open(this.filename, "a+");
		const readStream = fs.createReadStream(this.filename, {
			encoding: "utf8",
			fd: this._fd,
			autoClose: false,
		});
		const rl = readline.createInterface(readStream);
		let lineNo = 0;
		this._uncompressedSize = 0;

		try {
			await new Promise<void>((resolve, reject) => {
				rl.on("line", (line) => {
					// Count source lines for the error message
					lineNo++;
					// Skip empty lines
					if (!line) return;
					try {
						this._uncompressedSize++;
						this.parseLine(line);
					} catch (e) {
						if (this.options.ignoreReadErrors === true) {
							return;
						} else {
							reject(
								new Error(
									`Cannot open file: Invalid data in line ${lineNo}`,
								),
							);
						}
					}
				});
				rl.on("close", resolve);
			});
		} finally {
			// Close the file again to avoid EBADF
			rl.close();
			await fs.close(this._fd);
			this._fd = undefined;
		}

		const { onOpen, intervalMs, intervalMinChanges = 1 } =
			this.options.autoCompress ?? {};

		// If the DB should be compressed while opening, do it before starting the write thread
		if (onOpen) {
			await this.compressInternal();
		}

		// Start the write thread
		this._openPromise = createDeferredPromise();
		void this.writeThread();
		await this._openPromise;
		this._isOpen = true;

		// Start regular auto-compression
		if (intervalMs) {
			this._compressInterval = setInterval(() => {
				if (this._changesSinceLastCompress >= intervalMinChanges) {
					void this.compress();
				}
			}, intervalMs);
		}
	}

	/** Parses a line and updates the internal DB correspondingly */
	private parseLine(line: string): void {
		const record: { k: string; v?: V } = JSON.parse(line);
		const { k, v } = record;
		if (v !== undefined) {
			this._db.set(
				k,
				typeof this.options.reviver === "function"
					? this.options.reviver(k, v)
					: v,
			);
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
			this.write(this.entryToLine(key, value), true);
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

	private updateStatistics(command: string): void {
		if (command === "") {
			this._uncompressedSize = 0;
		} else {
			this._uncompressedSize++;
		}
		this._changesSinceLastCompress++;
	}

	private needToCompress(): boolean {
		// compression is busy?
		if (this.compressPromise) return false;
		const {
			sizeFactor = Number.POSITIVE_INFINITY,
			sizeFactorMinimumSize = 0,
		} = this.options.autoCompress ?? {};
		if (
			this.uncompressedSize >= sizeFactorMinimumSize &&
			this.uncompressedSize >= sizeFactor * this.size
		) {
			return true;
		}
		return false;
	}

	private cork(): void {
		/* istanbul ignore else - this is impossible to test */
		if (this._writeBacklog && this._writeCorkCount === 0) {
			this._writeBacklog.cork();
			this._writeCorkCount++;
		}
	}

	private uncork(): void {
		if (this._writeCorkCount > 0 && this._writeCorkTimeout) {
			clearTimeout(this._writeCorkTimeout);
			this._writeCorkTimeout = undefined;
		}
		while (this._writeBacklog && this._writeCorkCount > 0) {
			this._writeBacklog.uncork();
			this._writeCorkCount--;
		}
	}

	private autoCork(): void {
		if (!this.options.throttleFS?.intervalMs) return;

		const maybeUncork = (): void => {
			if (this._writeBacklog && this._writeBacklog.writableLength > 0) {
				// This gets the stream flowing again. The write thread will call
				// autoCork when it is done
				this.uncork();
			} else {
				// Nothing to uncork, schedule the next timeout
				this._writeCorkTimeout?.refresh();
			}
		};
		// Cork once and schedule the uncork
		this.cork();
		this._writeCorkTimeout =
			this._writeCorkTimeout?.refresh() ??
			setTimeout(maybeUncork, this.options.throttleFS.intervalMs);
	}

	/**
	 * Writes a line into the correct backlog
	 * @param noAutoCompress Whether auto-compression should be disabled
	 */
	private write(line: string, noAutoCompress: boolean = false): void {
		/* istanbul ignore else */
		if (this._compressBacklog && !this._compressBacklog.destroyed) {
			// The compress backlog handling also handles the file statistics
			this._compressBacklog.write(line);
		} else if (this._writeBacklog && !this._writeBacklog.destroyed) {
			// Update line statistics
			this.updateStatistics(line);

			// Either compress or write to the main file, never both
			if (!noAutoCompress && this.needToCompress()) {
				this.compress();
			} else {
				this._writeBacklog.write(line);
				// If this is a throttled stream, uncork it as soon as the write
				// buffer is larger than configured
				if (
					this.options.throttleFS?.maxBufferedCommands != undefined &&
					this._writeBacklog.writableLength >
						this.options.throttleFS.maxBufferedCommands
				) {
					this.uncork();
				}
			}
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
			return JSON.stringify({
				k: key,
				v: this.options.serializer?.(key, value) ?? value,
			});
		} else {
			return JSON.stringify({ k: key });
		}
	}

	/** Saves a compressed copy of the DB into `<filename>.dump` */
	public async dump(): Promise<void> {
		this._dumpPromise = createDeferredPromise();
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
		this._dumpPromise.resolve();
	}

	/** Asynchronously performs all write actions */
	private async writeThread(): Promise<void> {
		// This must be called before any awaits
		this._writeBacklog = new stream.PassThrough({ objectMode: true });
		this.autoCork();

		this._writePromise = createDeferredPromise();
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
			// When this is a throttled stream, auto-cork it when it was drained
			if (this._writeBacklog.readableLength === 0 && this._isOpen) {
				this.autoCork();
			}
		}
		this._writeBacklog.destroy();
		// The write backlog was closed, this means that the DB is being closed
		// close the file and resolve the close promise
		await fs.close(this._fd);
		this._writePromise.resolve();
	}

	private compressPromise: DeferredPromise<void> | undefined;
	private async compressInternal(): Promise<void> {
		if (this.compressPromise) return;

		this.compressPromise = createDeferredPromise();
		// Immediately remember the database size or writes while compressing
		// will be incorrectly reflected
		this._uncompressedSize = this.size;
		this._changesSinceLastCompress = 0;
		await this.dump();
		// After dumping, restart the write thread so no duplicate entries get written
		// Disable writing into the backlog stream and buffer all writes
		// in the compress backlog in the meantime
		this._compressBacklog = new stream.PassThrough({ objectMode: true });
		this.uncork();

		if (this._writeBacklog) {
			this._writeBacklog.end();
			await this._writePromise;
			this._writeBacklog = undefined;
		}

		// Replace the aof file
		await fs.move(this.filename, this.filename + ".bak");
		await fs.move(this.dumpFilename, this.filename);
		await fs.unlink(this.filename + ".bak");

		if (this._isOpen) {
			// Start the write thread again
			this._openPromise = createDeferredPromise();
			void this.writeThread();
			await this._openPromise;
		}

		// In case there is any data in the backlog stream, persist that too
		let line: string;
		while (null !== (line = this._compressBacklog.read())) {
			this.updateStatistics(line);
			this._writeBacklog!.write(line);
		}
		this._compressBacklog.destroy();
		this._compressBacklog = undefined;

		// If any method is waiting for the compress process, signal it that we're done
		this.compressPromise.resolve();
		this.compressPromise = undefined;
	}

	/** Compresses the db by dumping it and overwriting the aof file. */
	public async compress(): Promise<void> {
		if (!this._isOpen) return;

		return this.compressInternal();
	}

	/** Resolves when the `writeThread()` is finished */
	private _writePromise: DeferredPromise<void> | undefined;
	/** Resolves when the `dump()` method is finished */
	private _dumpPromise: DeferredPromise<void> | undefined;

	/** Closes the DB and waits for all data to be written */
	public async close(): Promise<void> {
		this._isOpen = false;
		if (this._compressInterval) clearInterval(this._compressInterval);
		if (this._writeCorkTimeout) clearTimeout(this._writeCorkTimeout);

		if (this.compressPromise) {
			// Wait until any pending compress processes are complete
			await this.compressPromise;
		} else if (this.options.autoCompress?.onClose) {
			// Compress if required
			await this.compressInternal();
		}

		// Disable writing into the backlog stream and wait for the write process to finish
		if (this._writeBacklog) {
			this.uncork();
			this._writeBacklog.end();
			await this._writePromise;
		}

		// Also wait for a potential dump process to finish
		/* istanbul ignore next - this is impossible to test since it requires exact timing */
		if (this._dumpBacklog) {
			// Disable writing into the dump backlog stream
			this._dumpBacklog.end();
			await this._dumpPromise;
		}

		// Reset all variables
		this._writePromise = undefined;
		this._dumpPromise = undefined;
		this._db.clear();
		this._fd = undefined;
		this._dumpFd = undefined;
		this._changesSinceLastCompress = 0;
		this._uncompressedSize = Number.NaN;
		this._writeCorkCount = 0;
	}
}
