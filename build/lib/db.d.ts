export declare class DB<V extends unknown = unknown> {
    constructor(filename: string);
    readonly filename: string;
    readonly dumpFilename: string;
    private _db;
    forEach: Map<string, V>["forEach"];
    get: Map<string, V>["get"];
    has: Map<string, V>["has"];
    [Symbol.iterator]: () => IterableIterator<[string, V]>;
    entries: Map<string, V>["entries"];
    keys: Map<string, V>["keys"];
    values: Map<string, V>["values"];
    get size(): number;
    private _fd;
    private _dumpFd;
    private _writeBacklog;
    private _dumpBacklog;
    /** Opens a file and allows iterating over all lines */
    private readLines;
    /** Opens the database file or creates it if it doesn't exist */
    open(): Promise<void>;
    /** Parses a line and updates the internal DB correspondingly */
    private parseLine;
    clear(): void;
    delete(key: string): boolean;
    set(key: string, value: V): this;
    private write;
    private entryToLine;
    /** Saves a compressed copy of the DB into `<filename>.dump` */
    dump(): Promise<void>;
    /** Asynchronously performs all write actions */
    private writeThread;
    /** Compresses the db by dumping it and overwriting the aof file. */
    compress(): Promise<void>;
    private _closeDBPromise;
    private _closeDumpPromise;
    /** Closes the DB and waits for all data to be written */
    close(): Promise<void>;
}
//# sourceMappingURL=db.d.ts.map