export declare class DB<V extends unknown = unknown> {
    readonly filename: string;
    constructor(filename: string);
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
    private _writeLog;
    /** Opens a file and allows iterating over all lines */
    private readLines;
    /** Opens the database file or creates it if it doesn't exist */
    open(): Promise<void>;
    /** Parses a line and updates the internal DB correspondingly */
    private parseLine;
    clear(): void;
    delete(key: string): boolean;
    set(key: string, value: V): this;
    /** Asynchronously performs all write actions */
    private writeThread;
    private _closePromise;
    /** Closes the DB and waits for all data to be written */
    close(): Promise<void>;
}
//# sourceMappingURL=db.d.ts.map