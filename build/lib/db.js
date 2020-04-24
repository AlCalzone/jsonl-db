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
        this.filename = filename;
        this._db = new Map();
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
        this._writeLog.write("");
    }
    delete(key) {
        const ret = this._db.delete(key);
        if (ret) {
            // Something was deleted
            this._writeLog.write(JSON.stringify({ k: key }));
        }
        return ret;
    }
    set(key, value) {
        this._db.set(key, value);
        this._writeLog.write(JSON.stringify({ k: key, v: value }));
        return this;
    }
    /** Asynchronously performs all write actions */
    async writeThread() {
        var e_2, _a;
        var _b;
        // TODO: use cork() and uncork()
        this._writeLog = new stream.PassThrough({ objectMode: true });
        try {
            for (var _c = __asyncValues(this._writeLog), _d; _d = await _c.next(), !_d.done;) {
                const action = _d.value;
                if (action === "") {
                    await fs.ftruncate(this._fd);
                }
                else {
                    await fs.write(this._fd, action + "\n");
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
        // The write log was closed, this means that the DB is being closed
        // close the file and resolve the close promise
        await fs.close(this._fd);
        (_b = this._closePromise) === null || _b === void 0 ? void 0 : _b.resolve();
    }
    /** Closes the DB and waits for all data to be written */
    async close() {
        if (this._writeLog) {
            this._closePromise = deferred_promise_1.createDeferredPromise();
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
exports.DB = DB;
//# sourceMappingURL=db.js.map