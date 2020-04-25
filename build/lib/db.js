"use strict";
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
const deferred_promise_1 = require("alcalzone-shared/deferred-promise");
const fs = require("fs-extra");
const readline = require("readline");
const stream = require("stream");
class DB {
    constructor(filename) {
        this._db = new Map();
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
    get size() {
        return this._db.size;
    }
    /** Opens a file and allows iterating over all lines */
    readLines() {
        const output = new stream.PassThrough({ objectMode: true });
        const readStream = fs.createReadStream(this.filename, {
            encoding: "utf8",
            fd: this._fd,
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
    /** Opens the database file or creates it if it doesn't exist */
    async open() {
        var e_1, _a;
        // Open the file for appending and reading
        this._fd = await fs.open(this.filename, "a+");
        try {
            for (var _b = __asyncValues(this.readLines()), _c; _c = await _b.next(), !_c.done;) {
                const line = _c.value;
                this.parseLine(line);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) await _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
        // Start the write thread
        this.writeThread();
    }
    /** Parses a line and updates the internal DB correspondingly */
    parseLine(line) {
        const record = JSON.parse(line);
        const { k, v } = record;
        if (v !== undefined) {
            this._db.set(k, v);
        }
        else {
            this._db.delete(k);
        }
    }
    clear() {
        this._db.clear();
        this.write("");
    }
    delete(key) {
        const ret = this._db.delete(key);
        if (ret) {
            // Something was deleted
            this.write(this.entryToLine(key));
        }
        return ret;
    }
    set(key, value) {
        this._db.set(key, value);
        this.write(this.entryToLine(key, value));
        return this;
    }
    write(line) {
        this._writeBacklog.write(line);
        // If necessary, write to the dump backlog, so the dump doesn't miss any data
        if (this._dumpBacklog && !this._dumpBacklog.destroyed) {
            this._dumpBacklog.write(line);
        }
    }
    entryToLine(key, value) {
        if (value !== undefined) {
            return JSON.stringify({ k: key, v: value });
        }
        else {
            return JSON.stringify({ k: key });
        }
    }
    /** Saves a compressed copy of the DB into `<filename>.dump` */
    async dump() {
        this._closeDumpPromise = deferred_promise_1.createDeferredPromise();
        // Open the file for writing (or truncate if it exists)
        this._dumpFd = await fs.open(this.dumpFilename, "w+");
        // And start dumping the DB
        // Start by creating a dump backlog, so parallel writes will be remembered
        this._dumpBacklog = new stream.PassThrough({ objectMode: true });
        // Create a copy of the other entries in the DB
        const entries = [...this._db];
        // And persist them
        for (const [key, value] of entries) {
            await fs.appendFile(this._dumpFd, this.entryToLine(key, value) + "\n");
        }
        // In case there is any data in the backlog stream, persist that too
        let line;
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
    async writeThread() {
        var e_2, _a;
        var _b;
        // TODO: use cork() and uncork()
        this._writeBacklog = new stream.PassThrough({ objectMode: true });
        try {
            for (var _c = __asyncValues(this._writeBacklog), _d; _d = await _c.next(), !_d.done;) {
                const action = _d.value;
                if (action === "") {
                    // Since we opened the file in append mode, we cannot truncate
                    // therefore close and open in write mode again
                    await fs.close(this._fd);
                    this._fd = await fs.open(this.filename, "w+");
                }
                else {
                    await fs.appendFile(this._fd, action + "\n");
                }
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (_d && !_d.done && (_a = _c.return)) await _a.call(_c);
            }
            finally { if (e_2) throw e_2.error; }
        }
        // The write backlog was closed, this means that the DB is being closed
        // close the file and resolve the close promise
        await fs.close(this._fd);
        (_b = this._closeDBPromise) === null || _b === void 0 ? void 0 : _b.resolve();
    }
    /** Compresses the db by dumping it and overwriting the aof file. */
    async compress() {
        await this.dump();
        // After dumping, cork the write backlog, so nothing gets written
        this._writeBacklog.cork();
        await fs.close(this._fd);
        // Replace the aof file
        await fs.move(this.filename, this.filename + ".bak");
        await fs.move(this.dumpFilename, this.filename);
        await fs.unlink(this.filename + ".bak");
        // Re-open the file for appending
        this._fd = await fs.open(this.filename, "a+");
        // and allow writing again
        this._writeBacklog.uncork();
    }
    /** Closes the DB and waits for all data to be written */
    async close() {
        var _a;
        if (this._writeBacklog) {
            this._closeDBPromise = deferred_promise_1.createDeferredPromise();
            // Disable writing into the backlog stream
            this._writeBacklog.end();
            this._writeBacklog = undefined;
            // Disable writing into the dump backlog stream
            (_a = this._dumpBacklog) === null || _a === void 0 ? void 0 : _a.end();
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
exports.DB = DB;
//# sourceMappingURL=db.js.map