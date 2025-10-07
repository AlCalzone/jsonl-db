import * as lockfile from "@alcalzone/proper-lockfile";
import { wait } from "alcalzone-shared/async";
import {
	createDeferredPromise,
	DeferredPromise,
} from "alcalzone-shared/deferred-promise";
import * as fs from "fs-extra";
import * as path from "path";
import * as readline from "readline";
import { Signal } from "./signal.js";

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

	/** Whether timestamps should be recorded when setting values. Default: false */
	enableTimestamps?: boolean;

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

	/** Configure settings related to the lockfile */
	lockfile?: Partial<{
		/**
		 * Override in which directory the lockfile is created.
		 * Defaults to the directory in which the DB file is located.
		 */
		directory?: string;

		/** Duration after which the lock is considered stale. Minimum: 5000, Default: 10000 */
		staleMs?: number;
		/** The interval in which the lockfile's `mtime` will be updated. Range: [1000...staleMs/2]. Default: staleMs/2  */
		updateMs?: number;
		/**
		 * How often to retry acquiring a lock before giving up. The retries progressively wait longer with an exponential backoff strategy.
		 * Range: [0...10]. Default: 0
		 */
		retries?: number;
		/** The start interval used for retries. Default: updateMs/2 */
		retryMinTimeoutMs?: number;
	}>;

	/**
	 * @deprecated Use lockfile.directory instead.
	 *
	 * Override in which directory the lockfile is created.
	 * Defaults to the directory in which the DB file is located.
	 */
	lockfileDirectory?: string;
}

/** This is the same as `fs-extra`'s WriteOptions */
export interface FsWriteOptions {
	encoding?: string | null;
	flag?: string;
	mode?: number;
	fs?: object;
	replacer?: any;
	spaces?: number | string;
	EOL?: string;
}

enum Operation {
	Clear = 0,
	Write = 1,
	Delete = 2,
}

type LazyEntry<V = unknown> = (
	| {
			op: Operation.Clear;
	  }
	| {
			op: Operation.Delete;
			key: string;
	  }
	| {
			op: Operation.Write;
			key: string;
			value: V;
			timestamp?: number;
	  }
) & {
	serialize(): string;
};

type PersistenceTask =
	// The DB is stopped, perform cleanup
	| { type: "stop" }
	// Used only in the persistence thread and indicates that a write should be performed
	| { type: "write" }
	// The DB should be dumped to a file
	| {
			type: "dump";
			filename: string;
			done: DeferredPromise<void>;
	  }
	// The DB should be compressed
	| {
			type: "compress";
			done: DeferredPromise<void>;
	  };

/**
 * fsync on a directory ensures there are no rename operations etc. which haven't been persisted to disk.
 */
/* istanbul ignore next - This is impossible to test */
async function fsyncDir(dirname: string): Promise<void> {
	// Windows will cause `EPERM: operation not permitted, fsync`
	// for directories, so don't do this

	if (process.platform === "win32") return;
	const fd = await fs.open(dirname, "r");
	await fs.fsync(fd);
	await fs.close(fd);
}

function getCurrentErrorStack(): string {
	const tmp = { message: "" };
	Error.captureStackTrace(tmp);
	return (tmp as any).stack.split("\n").slice(2).join("\n");
}

export class JsonlDB<V = unknown> {
	public constructor(filename: string, options: JsonlDBOptions<V> = {}) {
		this.validateOptions(options);

		this.filename = filename;
		this.dumpFilename = this.filename + ".dump";
		this.backupFilename = this.filename + ".bak";
		const lockfileDirectory =
			options.lockfile?.directory ?? options.lockfileDirectory;
		this.lockfileName = lockfileDirectory
			? path.join(lockfileDirectory, path.basename(this.filename))
			: this.filename;

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
		if (options.lockfile) {
			const {
				directory,
				retries,
				staleMs = 10000,
				updateMs = staleMs / 2,
				retryMinTimeoutMs,
			} = options.lockfile;
			if (staleMs < 2000) {
				throw new Error("staleMs must be >= 2000");
			}
			if (updateMs < 1000) {
				throw new Error("updateMs must be >= 1000");
			}
			if (updateMs > staleMs / 2) {
				throw new Error(`updateMs must be <= ${staleMs / 2}`);
			}
			if (retries != undefined && retries < 0) {
				throw new Error("retries must be >= 0");
			}
			if (retries != undefined && retries > 10) {
				throw new Error("retries must be <= 10");
			}
			if (retryMinTimeoutMs != undefined && retryMinTimeoutMs < 100) {
				throw new Error("retryMinTimeoutMs must be >= 100");
			}
			if (
				options.lockfileDirectory != undefined &&
				directory != undefined
			) {
				throw new Error(
					"lockfileDirectory and lockfile.directory must not both be specified",
				);
			}
		}
	}

	public readonly filename: string;
	public readonly dumpFilename: string;
	public readonly backupFilename: string;
	private readonly lockfileName: string;

	private options: JsonlDBOptions<V>;

	private _db = new Map<string, V>();
	private _timestamps = new Map<string, number>();
	// Declare all map properties we can use directly
	declare forEach: Map<string, V>["forEach"];
	declare get: Map<string, V>["get"];
	declare has: Map<string, V>["has"];
	declare [Symbol.iterator]: () => IterableIterator<[string, V]>;
	declare entries: Map<string, V>["entries"];
	declare keys: Map<string, V>["keys"];
	declare values: Map<string, V>["values"];

	public getTimestamp(key: string): number | undefined {
		return this._timestamps.get(key);
	}

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
	private _compressBySizeThreshold: number = Number.POSITIVE_INFINITY;
	// Signals that the conditions to compress the DB by size are fulfilled
	private _compressBySizeNeeded = new Signal();
	// Signals that the minimum number of changes to automatically compress were exceeded
	private _compressIntervalMinChangesExceeded = false;
	// Signals that the next change may immediately trigger a write to disk
	private _writeIntervalElapsed = false;
	// Signals that the journal has exceeded the maximum buffered commands
	// or that the journal contains entries that may be written to disk immediately
	private _journalFlushable = new Signal();

	private updateCompressBySizeThreshold(): void {
		if (!this.options.autoCompress) return;
		if (!this.options.autoCompress.sizeFactor) return;
		const {
			sizeFactor = Number.POSITIVE_INFINITY,
			sizeFactorMinimumSize = 0,
		} = this.options.autoCompress;

		this._compressBySizeThreshold = Math.max(
			sizeFactorMinimumSize,
			sizeFactor * this.size,
		);
	}

	private triggerJournalFlush(): void {
		// Trigger a flush...
		if (
			// ... immediately if writing isn't throttled
			!this.options.throttleFS?.intervalMs ||
			// ... immediately if the timer elapsed
			this._writeIntervalElapsed ||
			// ... or the maximum buffered commands were exceeded
			this.exceededMaxBufferedCommands()
		) {
			this._journalFlushable.set();
		}
	}

	private _isOpen: boolean = false;
	public get isOpen(): boolean {
		return this._isOpen;
	}

	// Resolves when the persistence thread ends
	private _persistencePromise: Promise<void> | undefined;
	// An array of tasks to be handled by the persistence thread
	private _persistenceTasks: PersistenceTask[] = [];
	// Indicates to the persistence thread that there is a pending task
	private _persistenceTaskSignal = new Signal();

	private _journal: LazyEntry<V>[] = [];
	private _fd: number | undefined;

	private drainJournal(): LazyEntry<V>[] {
		return this._journal.splice(0, this._journal.length);
	}

	private pushPersistenceTask(task: PersistenceTask): void {
		this._persistenceTasks.push(task);
		this._persistenceTaskSignal.set();
	}

	private _openPromise: DeferredPromise<void> | undefined;
	// /** Opens the database file or creates it if it doesn't exist */
	public async open(): Promise<void> {
		// Open the file for appending and reading
		await fs.ensureDir(path.dirname(this.filename));

		let retryOptions: lockfile.LockOptions["retries"];
		if (this.options.lockfile?.retries) {
			retryOptions = {
				minTimeout:
					this.options.lockfile.retryMinTimeoutMs ??
					(this.options.lockfile.updateMs ?? 2000) / 2,
				retries: this.options.lockfile.retries,
				factor: 1.25,
			};
		}

		try {
			await fs.ensureDir(path.dirname(this.lockfileName));
			await lockfile.lock(this.lockfileName, {
				// We cannot be sure that the file exists before acquiring the lock
				realpath: false,

				stale:
					// Avoid timeouts during testing
					process.env.NODE_ENV === "test"
						? 100000
						: /* istanbul ignore next - this is impossible to test */ this
								.options.lockfile?.staleMs,
				update: this.options.lockfile?.updateMs,
				retries: retryOptions,

				onCompromised: /* istanbul ignore next */ () => {
					// do nothing
				},
			});
		} catch (e) {
			throw new Error(`Failed to lock DB file "${this.lockfileName}"!`);
		}

		// If the application crashed previously, try to recover from it
		await this.tryRecoverDBFiles();

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
				const actualLines = new Map<
					string,
					[lineNo: number, line: string]
				>();
				rl.on("line", (line) => {
					// Count source lines for the error message
					lineNo++;
					// Skip empty lines
					if (!line) return;
					try {
						this._uncompressedSize++;
						// Extract the key and only remember the last line for each one
						const key = this.parseKey(line);
						actualLines.set(key, [lineNo, line]);
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
				rl.on("close", () => {
					// We've read all lines, now only parse those that contain useful data
					for (const [lineNo, line] of actualLines.values()) {
						try {
							this.parseLine(line);
						} catch (e) {
							if (this.options.ignoreReadErrors === true) {
								continue;
							} else {
								reject(
									new Error(
										`Cannot open file: Invalid data in line ${lineNo}`,
									),
								);
							}
						}
					}
					resolve();
				});
			});
		} finally {
			// Close the file again to avoid EBADF
			rl.close();
			await fs.close(this._fd);
			this._fd = undefined;
		}

		this.updateCompressBySizeThreshold();

		// Start background persistence thread
		this._persistencePromise = this.persistenceThread();
		await this._openPromise;
		this._isOpen = true;

		// If the DB should be compressed while opening, do it now
		if (this.options.autoCompress?.onOpen) await this.compress();
	}

	/**
	 * Makes sure that there are no remains of a previous broken compress attempt and restores
	 * a DB backup if it exists.
	 */
	private async tryRecoverDBFiles(): Promise<void> {
		// During the compression, the following sequence of events happens:
		// 1. A .jsonl.dump file gets written with a compressed copy of the data
		// 2. Files get renamed: .jsonl -> .jsonl.bak, .jsonl.dump -> .jsonl
		// 3. .bak file gets removed
		// 4. Buffered data gets written to the .jsonl file

		// This means if the .jsonl file is absent or truncated, we should be able to pick either the .dump or the .bak file
		// and restore the .jsonl file from it
		let dbFileIsOK = false;
		try {
			const dbFileStats = await fs.stat(this.filename);
			dbFileIsOK = dbFileStats.isFile() && dbFileStats.size > 0;
		} catch {
			// ignore
		}

		// Prefer the DB file if it exists, remove the others in case they exist
		if (dbFileIsOK) {
			try {
				await fs.remove(this.backupFilename);
			} catch {
				// ignore
			}
			try {
				await fs.remove(this.dumpFilename);
			} catch {
				// ignore
			}
			return;
		}

		// The backup file should have complete data - the dump file could be subject to an incomplete write
		let bakFileIsOK = false;
		try {
			const bakFileStats = await fs.stat(this.backupFilename);
			bakFileIsOK = bakFileStats.isFile() && bakFileStats.size > 0;
		} catch {
			// ignore
		}

		if (bakFileIsOK) {
			// Overwrite the broken db file with it and delete the dump file
			try {
				await fs.move(this.backupFilename, this.filename, {
					overwrite: true,
				});
				try {
					await fs.remove(this.dumpFilename);
				} catch {
					// ignore
				}
				return;
			} catch {
				// Moving failed, try the next possibility
			}
		}

		// Try the dump file as a last attempt
		let dumpFileIsOK = false;
		try {
			const dumpFileStats = await fs.stat(this.dumpFilename);
			dumpFileIsOK = dumpFileStats.isFile() && dumpFileStats.size > 0;
		} catch {
			// ignore
		}
		if (dumpFileIsOK) {
			try {
				// Overwrite the broken db file with the dump file and delete the backup file
				await fs.move(this.dumpFilename, this.filename, {
					overwrite: true,
				});
				try {
					await fs.remove(this.backupFilename);
				} catch {
					// ignore
				}
				return;
			} catch {
				// Moving failed
			}
		}
	}

	/** Reads a line and extracts the key without doing a full-blown JSON.parse() */
	private parseKey(line: string): string {
		if (0 !== line.indexOf(`{"k":"`)) {
			throw new Error("invalid data");
		}
		const keyStart = 6;
		let keyEnd = line.indexOf(`","v":`, keyStart);
		if (-1 === keyEnd) {
			// try again with a delete command
			if (line.endsWith(`"}`)) {
				keyEnd = line.length - 2;
			} else {
				throw new Error("invalid data");
			}
		}
		return line.slice(keyStart, keyEnd);
	}

	/** Parses a line and updates the internal DB correspondingly */
	private parseLine(line: string): void {
		const record: { k: string; v?: V; ts?: number } = JSON.parse(line);
		const { k, v, ts } = record;
		if (v !== undefined) {
			this._db.set(
				k,
				typeof this.options.reviver === "function"
					? this.options.reviver(k, v)
					: v,
			);
			if (this.options.enableTimestamps && ts !== undefined) {
				this._timestamps.set(k, ts);
			}
		} else {
			if (this._db.delete(k)) this._timestamps.delete(k);
		}
	}

	public clear(): void {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		this._db.clear();
		// All pending writes are obsolete, remove them from the journal
		this.drainJournal();
		this._journal.push(this.makeLazyClear());
	}

	public delete(key: string): boolean {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		const ret = this._db.delete(key);
		if (ret) {
			// Something was deleted
			this._journal.push(this.makeLazyDelete(key));
			this._timestamps.delete(key);

			this.updateCompressBySizeThreshold();
			this.triggerJournalFlush();
		}
		return ret;
	}

	public set(key: string, value: V, updateTimestamp: boolean = true): this {
		if (!this._isOpen) {
			throw new Error("The database is not open!");
		}
		this._db.set(key, value);
		if (this.options.enableTimestamps) {
			// If the timestamp should be updated, use the current time, otherwise try to preserve the old one
			let ts: number | undefined;
			if (updateTimestamp) {
				ts = Date.now();
				this._timestamps.set(key, ts);
			} else {
				ts = this._timestamps.get(key);
			}
			this._journal.push(this.makeLazyWrite(key, value, ts));
		} else {
			this._journal.push(this.makeLazyWrite(key, value));
		}

		this.updateCompressBySizeThreshold();
		this.triggerJournalFlush();

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
			// Importing JSON does not have timestamp information
			this._db.set(key, value);
			this._journal.push(this.makeLazyWrite(key, value));
		}

		this.updateCompressBySizeThreshold();
		this.triggerJournalFlush();
	}

	public async exportJson(
		filename: string,
		options?: FsWriteOptions,
	): Promise<void> {
		if (!this._isOpen) {
			return Promise.reject(new Error("The database is not open!"));
		}
		return fs.writeJSON(filename, Object.fromEntries([...this._db]), options);
	}

	private entryToLine(key: string, value?: V, timestamp?: number): string {
		if (value !== undefined) {
			const k = key;
			const v = this.options.serializer?.(key, value) ?? value;

			if (this.options.enableTimestamps && timestamp !== undefined) {
				return JSON.stringify({ k, v, ts: timestamp });
			} else {
				return JSON.stringify({ k, v });
			}
		} else {
			return JSON.stringify({ k: key });
		}
	}

	private makeLazyClear(): LazyEntry & { op: Operation.Clear } {
		return {
			op: Operation.Clear,

			serialize:
				/* istanbul ignore next - this is impossible to test since it requires exact timing */ () =>
					"",
		};
	}

	private makeLazyDelete(key: string): LazyEntry & { op: Operation.Delete } {
		let serialized: string | undefined;
		return {
			op: Operation.Delete,
			key,
			serialize: () => {
				if (serialized == undefined) {
					serialized = this.entryToLine(key);
				}
				return serialized;
			},
		};
	}

	private makeLazyWrite(
		key: string,
		value: V,
		timestamp?: number,
	): LazyEntry<V> & { op: Operation.Write } {
		let serialized: string | undefined;
		return {
			op: Operation.Write,
			key,
			value,
			timestamp,
			serialize: () => {
				if (serialized == undefined) {
					serialized = this.entryToLine(key, value, timestamp);
				}
				return serialized;
			},
		};
	}

	/**
	 * Saves a compressed copy of the DB into the given path.
	 *
	 * **WARNING:** This MUST be called from {@link persistenceThread}!
	 * @param targetFilename Where the compressed copy should be written. Default: `<filename>.dump`
	 * @param drainJournal Whether the journal should be drained when writing the compressed copy or simply cloned.
	 */
	private async dumpInternal(
		targetFilename: string = this.dumpFilename,
		drainJournal: boolean,
	): Promise<void> {
		// Open the file for writing (or truncate if it exists)
		const fd = await fs.open(targetFilename, "w+");

		// Create a copy of the other entries in the DB
		// Also, remember how many entries were in the journal. These are already part of
		// the map, so we don't need to append them later and keep a consistent state
		const entries = [...this._db];
		const timestamps = new Map([...this._timestamps]);
		const journalLength = this._journal.length;

		// And persist them
		let serialized = "";
		for (const [key, value] of entries) {
			// No need to serialize lazily here
			if (this.options.enableTimestamps && timestamps.has(key)) {
				serialized +=
					this.entryToLine(key, value, timestamps.get(key)) + "\n";
			} else {
				serialized += this.entryToLine(key, value) + "\n";
			}
		}
		await fs.appendFile(fd, serialized);

		// In case there is any new data in the journal, persist that too
		let journal = drainJournal
			? this._journal.splice(0, this._journal.length)
			: this._journal;
		journal = journal.slice(journalLength);
		await this.writeJournalToFile(fd, journal, false);

		await fs.close(fd);
	}

	/**
	 * Saves a compressed copy of the DB into the given path.
	 * @param targetFilename Where the compressed copy should be written. Default: `<filename>.dump`
	 */
	public async dump(
		targetFilename: string = this.dumpFilename,
	): Promise<void> {
		// Prevent dumping the DB when it is closed
		if (!this._isOpen) return;

		const done = createDeferredPromise();
		this.pushPersistenceTask({
			type: "dump",
			filename: targetFilename,
			done,
		});
		const stack = getCurrentErrorStack();
		try {
			await done;
		} catch (e: any) {
			e.stack += "\n" + stack;
			throw e;
		}
	}

	private exceededMaxBufferedCommands(): boolean {
		const maxBufferedCommands =
			this.options.throttleFS?.maxBufferedCommands;
		if (maxBufferedCommands == undefined) {
			return false;
		} else {
			return (
				this._journal.length > 0 &&
				this._journal.length > maxBufferedCommands
			);
		}
	}

	private needToCompressBySize(): boolean {
		if (!this._isOpen) return false;
		return this._uncompressedSize >= this._compressBySizeThreshold;
	}

	private needToCompressByTime(lastCompress: number): boolean {
		if (!this.options.autoCompress) return false;
		const { intervalMs, intervalMinChanges = 1 } =
			this.options.autoCompress;
		if (!intervalMs) return false;

		return (
			this._changesSinceLastCompress >= intervalMinChanges &&
			Date.now() - lastCompress >= intervalMs
		);
	}

	private async persistenceThread(): Promise<void> {
		const compressInterval = this.options.autoCompress?.intervalMs;
		const throttleInterval = this.options.throttleFS?.intervalMs ?? 0;

		// Keep track of the write accesses and compression attempts
		let lastWrite = Date.now();
		let lastCompress = Date.now();

		// Open the file for appending and reading
		this._fd = await fs.open(this.filename, "a+");
		this._openPromise?.resolve();

		while (true) {
			const now = Date.now();

			// Figure out how long the timeouts should be.
			// > 0 means wait, 0 means do it now, undefined means don't do it
			let compressByTimeSleepDuration: number | undefined;
			if (compressInterval) {
				const nextCompress = lastCompress + compressInterval;
				if (nextCompress > now) {
					compressByTimeSleepDuration = nextCompress - now;
				} else if (this._compressIntervalMinChangesExceeded) {
					// Compress now
					compressByTimeSleepDuration = 0;
				}
			}

			let throttledWriteSleepDuration: number | undefined;
			if (throttleInterval) {
				const nextWrite = lastWrite + throttleInterval;
				if (nextWrite > now) {
					throttledWriteSleepDuration = nextWrite - now;
				} else if (this._journal.length > 0) {
					// Write now
					throttledWriteSleepDuration = 0;
				} else {
					// Indicate to the outside that the next journal entry
					// should cause a write/trigger
					this._writeIntervalElapsed = true;
				}
			} else if (this._journal.length > 0) {
				// Not throttled, write immediately
				throttledWriteSleepDuration = 0;
			}

			// Figure out what to do
			type Input = "flush journal" | "write" | "compress" | "task";
			// We do this in two steps, as we only want to react to a single action
			const input = (await Promise.race(
				[
					// The journal has exceeded the maximum buffered commands
					// and needs to be written to disk
					this._journalFlushable.then(() => "flush journal" as const),

					// The timer to flush the journal to disk has elapsed
					throttledWriteSleepDuration != undefined &&
						wait(throttledWriteSleepDuration, true).then(
							() => "write" as const,
						),

					// The timer to compress by time has elapsed
					compressByTimeSleepDuration != undefined &&
						wait(compressByTimeSleepDuration, true).then(() => {
							if (this._compressIntervalMinChangesExceeded) {
								return "compress" as const;
							}
						}),

					this._compressBySizeNeeded.then(() => "compress" as const),

					// A task was received from the outside
					this._persistenceTaskSignal.then(() => "task" as const),
				].filter((p) => !!p),
			)) as Input;

			let task: PersistenceTask | undefined;
			if (input === "flush journal") {
				task = { type: "write" };
			} else if (input === "write") {
				task = { type: "write" };
			} else if (input === "compress") {
				// Need to compress
				task = {
					type: "compress",
					done: createDeferredPromise(),
				};
				// but catch errors!
				// eslint-disable-next-line @typescript-eslint/no-empty-function
				task.done.catch(() => {});
			} else if (input === "task") {
				task = this._persistenceTasks.shift();
				// Reset the signal when there are no more tasks
				if (!task) this._persistenceTaskSignal.reset();
			}

			if (!task) continue;

			let isStopCmd = false;
			switch (task.type) {
				case "stop":
					isStopCmd = true;
				// fall through
				case "write": {
					// Write to disk if necessary

					// Only write if there are actually entries to write
					const shouldWrite = this._journal.length > 0;

					if (shouldWrite) {
						// Drain the journal
						const journal = this.drainJournal();
						this._fd = await this.writeJournalToFile(
							this._fd,
							journal,
						);
						lastWrite = Date.now();
					}

					if (isStopCmd) {
						await fs.close(this._fd);
						this._fd = undefined;
						return;
					} else {
						// Since we wrote something, the uncompressed size may have changed
						if (this.needToCompressBySize()) {
							this._compressBySizeNeeded.set();
						}
						// Also we might have surpassed the change threshold to trigger a compression
						this._compressIntervalMinChangesExceeded =
							this._changesSinceLastCompress >=
							(this.options.autoCompress?.intervalMinChanges ??
								1);
					}
					break;
				}

				case "dump": {
					try {
						await this.dumpInternal(task.filename, false);
						task.done.resolve();
					} catch (e) {
						task.done.reject(e);
					}
					break;
				}

				case "compress": {
					try {
						await this.doCompress();
						lastCompress = Date.now();
						task.done?.resolve();
					} catch (e) {
						task.done?.reject(e);
					}
					break;
				}
			}
		}
	}

	/** Writes the given journal to the given file descriptor. Returns the new file descriptor if the file was re-opened during the process */
	private async writeJournalToFile(
		fd: number,
		journal: LazyEntry<V>[],
		updateStatistics: boolean = true,
	): Promise<number> {
		// The chunk map is used to buffer all entries that are currently waiting in line
		// so we avoid serializing redundant entries. When the writing is throttled,
		// the chunk map will only be used for a short time.
		const chunk = new Map<string, LazyEntry>();
		let serialized = "";
		let truncate = false;

		for (const entry of journal) {
			if (entry.op === Operation.Clear) {
				chunk.clear();
				truncate = true;
			} else {
				// Only remember the last entry for each key
				chunk.set(entry.key, entry);
			}
		}

		// When the journal has been drained, perform the necessary write actions
		if (truncate) {
			// Since we opened the file in append mode, we cannot truncate
			// therefore close and open in write mode again
			await fs.close(fd);
			fd = await fs.open(this.filename, "w+");
			truncate = false;
			if (updateStatistics) {
				// Now the DB size is effectively 0 and we have no "uncompressed" changes pending
				this._uncompressedSize = 0;
				this._changesSinceLastCompress = 0;
			}
		}
		// Collect all changes
		for (const entry of chunk.values()) {
			serialized += entry.serialize() + "\n";
			if (updateStatistics) {
				this._uncompressedSize++;
				this._changesSinceLastCompress++;
			}
		}
		// and write once, making sure everything is written
		await fs.appendFile(fd, serialized);
		await fs.fsync(fd);

		// Reset the signals related to writing the journal
		this._journalFlushable.reset();
		this._writeIntervalElapsed = false;

		return fd;
	}

	/**
	 * Compresses the db by dumping it and overwriting the aof file.
	 *
	 * **WARNING:** This MUST be called from {@link persistenceThread}!
	 */
	private async doCompress(): Promise<void> {
		// 1. Ensure the backup contains everything in the DB and journal
		const journal = this.drainJournal();
		this._fd = await this.writeJournalToFile(this._fd!, journal);
		await fs.close(this._fd);
		this._fd = undefined;

		// 2. Create a dump, draining the journal to avoid duplicate writes
		await this.dumpInternal(this.dumpFilename, true);

		// We're done writing, so update the staticstics now
		this._uncompressedSize = this._db.size;
		this.updateCompressBySizeThreshold();
		this._changesSinceLastCompress = 0;
		this._compressIntervalMinChangesExceeded = false;

		// 3. Ensure there are no pending rename operations or file creations
		await fsyncDir(path.dirname(this.filename));

		// 4. Swap files around, then ensure the directory entries are written to disk
		await fs.move(this.filename, this.backupFilename, {
			overwrite: true,
		});
		await fs.move(this.dumpFilename, this.filename, { overwrite: true });
		await fsyncDir(path.dirname(this.filename));

		// 5. Delete backup
		await fs.unlink(this.backupFilename);

		// 6. open the main DB file again in append mode
		this._fd = await fs.open(this.filename, "a+");

		// Reset the signals related to compressing the DB
		this._compressBySizeNeeded.reset();
	}

	/** Compresses the db by dumping it and overwriting the aof file. */
	public async compress(): Promise<void> {
		if (!this._isOpen) return;
		await this.compressInternal();
	}

	/** Compresses the db by dumping it and overwriting the aof file. */
	private async compressInternal(): Promise<void> {
		// Avoid having multiple compress operations running in parallel
		const task = this._persistenceTasks.find(
			(t): t is PersistenceTask & { type: "compress" } =>
				t.type === "compress",
		);
		if (task) return task.done;

		const done = createDeferredPromise<void>();
		this.pushPersistenceTask({
			type: "compress",
			done,
		});
		const stack = getCurrentErrorStack();
		try {
			await done;
		} catch (e: any) {
			e.stack += "\n" + stack;
			throw e;
		}
	}

	/** Closes the DB and waits for all data to be written */
	public async close(): Promise<void> {
		if (!this._isOpen) return;
		this._isOpen = false;

		// Compress on close if required
		if (this.options.autoCompress?.onClose) {
			await this.compressInternal();
		}

		// Stop persistence thread and wait for it to finish
		this.pushPersistenceTask({ type: "stop" });
		await this._persistencePromise;

		// Reset all variables
		this._db.clear();
		this._changesSinceLastCompress = 0;
		this._uncompressedSize = Number.NaN;
		this.updateCompressBySizeThreshold();

		// Free the lock
		try {
			if (await lockfile.check(this.lockfileName, { realpath: false }))
				await lockfile.unlock(this.lockfileName, { realpath: false });
		} catch {
			// whatever - just don't crash
		}
	}
}
