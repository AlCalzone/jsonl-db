# jsonl-db

Simple JSONL-based key-value store. Uses an append-only file to store the data. With support for database dumps and compressing the db file.

![Build Status](https://action-badges.now.sh/AlCalzone/jsonl-db)
<!-- [![Coverage Status](https://img.shields.io/coveralls/github/AlCalzone/jsonl-db.svg)](https://coveralls.io/github/AlCalzone/jsonl-db) -->
[![node](https://img.shields.io/node/v/@alcalzone/jsonl-db.svg) ![npm](https://img.shields.io/npm/v/@alcalzone/jsonl-db.svg)](https://www.npmjs.com/package/@alcalzone/jsonl-db)


## Usage

Load the module:

```ts
import { DB } from "@alcalzone/jsonl-db";
```

Open or create a database file and use it like a  [`Map`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map)

```ts
// Open
const db = new DB("/path/to/file");
await db.open();
// db.isOpen is now true

// and use it
db.set("key", value);
db.delete("key");
db.clear();
if (db.has("key")) {
	result = db.get("key");
}
// ...forEach, keys(), entries(), values(), ...
```

### Handling invalid data

If corrupt data is encountered while opening the DB, the call to `open()` will be rejected. If this is to be expected, use the options parameter on the constructor to turn on forgiving behavior:
```ts
const db = new DB("/path/to/file", { ignoreReadErrors: true });
await db.open();
```
**Warning:** This may result in inconsistent data since invalid lines are silently ignored.

### Support custom objects/values

You can optionally transform the parsed values by passing a reviver function. This allows storing non-primitive objects in the database if those can be transformed to JSON (e.g. by overwriting the `toJSON` method). To control the transformation values before they are saved to the database, use the serializer function. This is necessary for `Map`s, `Set`s, `WeakMap`s and `WeakSet`s.
```ts
function reviver(key: string, serializedValue: any) {
	// MUST return a value. If you don't want to transform `serializedValue`, return it.
}

function serializer(key: string, value: any) {
	// MUST return a value. If you don't want to transform `value`, return it.
}

const db = new DB("/path/to/file", { reviver, serializer });
await db.open();
```

### Closing the database

Data written to the DB is persisted asynchronously. Be sure to call `close()` when you no longer need the database in order to flush all pending writes and close all files:

```ts
await db.close();
```
Now, `db.isOpen` is `false`. While the db is not open, any calls that access the data will throw an error.

### Controlling file system access

By default, the database immediately writes to the database file. You can throttle the write accesses using the `throttleFS` constructor option. Be aware that buffered data will be lost in case the process crashes.
```ts
const db = new DB("/path/to/file", { throttleFS: { /* throttle options */ } });
```
The following options exist:
| Option | Default | Description |
|-----------------|---------|-------------|
| `intervalMs` | `0` | Write to the database file no more than every `intervalMs` milliseconds. |
| `maxBufferedCommands` | `+Infinity` | Force a write after `maxBufferedCommands` have been buffered. This reduces memory consumption and data loss in case of a crash. |

To create a compressed copy of the database in `/path/to/file.dump`, use the `dump()` method. If any data is written to the db during the dump, it is appended to the dump but most likely compressed.

### Lockfile-related options

A lockfile is used to avoid concurrent access to the DB file. Multiple options exist to control where this lockfile is created and how it is accessed:
```ts
const db = new DB("/path/to/file", { lockfile: { /* lockfile options */ } });
```

| Option | Default | Description |
|-----------------|---------|-------------|
| `directory` | - | Change where the lockfile is created, e.g. to put the lockfile into a `tmpfs`. By default the lockfile is created in the same directory as the DB file. If the directory does not exist, it will be created when opening the DB. |
| `staleMs` | `10000` | Duration after which the lock is considered stale. Minimum: `2000` |
| `updateMs` | `staleMs/2` | The interval in which the lockfile's `mtime` will be updated. Range: `1000 ... staleMs/2` |
| `retries` | `0` | How often to retry acquiring a lock before giving up. The retries progressively wait longer with an exponential backoff strategy. |
| `retryMinTimeoutMs` | `updateMs/2` or `1000` | The start interval used for retries. Minimum: `100` |


### Copying and compressing the database

```ts
await db.dump(); // To use the default dump filename `/path/to/file.dump`
await db.dump("/somewhere/else.jsonl"); // To use a different filename
```

After a while, the main db file may contain unnecessary entries. The raw number of entries can be read using the `uncompressedSize` property. To remove unnecessary entries, use the `compress()` method.

```ts
await db.compress();
```

**Note:** During this call, `/path/to/file.dump` is overwritten and then renamed, `/path/to/file.bak` is overwritten and then deleted. So make sure you don't have any important data in these files.

The database can automatically compress the database file under some conditions. To do so, use the `autoCompress` parameter of the constructor options:
```ts
const db = new DB("/path/to/file", { autoCompress: { /* auto compress options */ }});
```
The following options exist (all optional) and can be combined:
| Option | Default | Description |
|-----------------|---------|-------------|
| `sizeFactor` | `+Infinity` | Compress when `uncompressedSize >= size * sizeFactor` |
| `sizeFactorMinimumSize` | 0 | Configure the minimum size necessary for auto-compression based on size |
| `intervalMs` | `+Infinity` | Compress after a certain time has passed |
| `intervalMinChanges` | 1 | Configure the minimum count of changes for auto-compression based on time |
| `onClose` | `false` | Compress when closing the DB |
| `onOpen` | `false` | Compress after opening the DB |

### Keeping track of timestamps

The DB can automatically keep track of the last time a key was written. To do so, set the `enableTimestamps` option to `true`. Every `set` call will then update the timestamp of a value, which can be retrieved using `getTimestamp(key)`. To opt out of this behavior for individual writes, pass `false` as the 3rd argument to `set`.

```ts
db.set("key", "value", false); // Don't update the timestamp
```

> **Note**
> Recording timestamps decreases the DB throughput by ~10...30% depending on how it is used.

### Import / Export

Importing JSON files can be done this way:
```ts
// pass a filename, the import will be asynchronous
await db.importJson(filename);
// pass the object directly, the import will be synchronous
db.importJson({key: "value"});
```
In both cases, existing entries in the DB will not be deleted but will be overwritten if they exist.

Exporting JSON files is also possible:
```ts
await db.exportJson(filename[, options]);
```
The file will be overwritten if it exists. The 2nd options argument can be used to control the file formatting. Since `fs-extra`'s `writeJson` is used under the hood, take a look at that [method documentation](https://github.com/jprichardson/node-fs-extra/blob/master/docs/writeJson.md) for details on the options object.

## Changelog

<!--
	Placeholder for next release:
	### __WORK IN PROGRESS__
-->
### 3.1.1 (2024-01-25)
* Reduced CPU load while idle (#475)

### 3.1.0 (2023-03-15)
* Support opt-out of updating timestamps for individual set calls (#415)

### 3.0.0 (2023-03-09)
* Support recording timestamps for each value (#413)
* Drop support for Node.js 12 (#414)

### 2.5.3 (2022-09-06)
* Consider lockfiles from the future to be stale (#371)

### 2.5.2 (2022-05-02)
* Dependency updates

### 2.5.1 (2022-02-13)
* Allow setting minimum retry timeout
* Correct minimum value for the lockfile's stale timeout

### 2.5.0 (2022-02-13)
* Add the ability to control the lockfile's stale/update timeouts and retrying acquiring a lock

### 2.4.3 (2022-02-09)
* Errors while automatically compressing the DB are now caught

### 2.4.2 (2022-02-09)
* Errors while automatically restoring the DB from a backup or dump are now caught
* Simplified and decoupled the persistence code. Individual commands like `dump` and `compress` are now properly sequenced and should no longer conflict with each other.
* Increased throughput for primitive entries by ~2x

### 2.4.1 (2021-12-30)
* Individual writes are now collected in a string and written at once, increasing throughput for larger entries by ~10x.

### 2.4.0 (2021-12-27)
* Stringifying the individual lines now happens lazily and only when actually necessary, increasing the throughput by 30...50x.

### 2.3.0 (2021-12-19)
* Add the ability to dump the database to a different location

### 2.2.0 (2021-10-15)
* Add the ability to specify where the lockfile is created

### 2.1.0 (2021-06-22)
* When opening the DB, recover from crashes that happened while compressing the DB
* Ensure that the DB files are flushed to disk when closing or renaming files
* Tests now work with the real filesystem instead of `mock-fs`, which breaks in Node.js `16.3+`
* Modernized TypeScript build output

### 2.0.0 (2021-06-19)
Update dependencies and drop support for Node.js 10

### 1.2.5 (2021-05-29)
Prevent opening one DB file in multiple instances of the DB using lockfiles

### 1.2.4 (2021-02-15)
Reduced the work done while opening a DB

### 1.2.3 (2021-01-02)
Fixed a crash that happens while compressing the DB when the `.bak` file exists

### 1.2.2 (2020-10-16)
When consuming this library without `skipLibCheck`, `@types/fs-extra` is no longer required

### 1.2.1 (2020-08-20)
Update dependencies

### 1.2.0 (2020-05-25)
Added an optional serializer function to transform non-primitive objects before writing to the DB file

### 1.1.2 (2020-05-11)
Fixed a timeout leak that would prevent Node.js from exiting

### 1.1.1 (2020-05-07)
Leading directories are now created if they don't exist

### 1.1.0 (2020-05-02)
Added functionality to throttle write accesses

### 1.0.1 (2020-04-29)
Export `JsonlDBOptions` from the main entry point

### 1.0.0 (2020-04-29)
Added auto-compress functionality

### 0.5.1 (2020-04-28)
Fix: The main export no longer exports `JsonlDB` as `DB`.

### 0.5.0 (2020-04-27)
Added an optional reviver function to transform non-primitive objects while loading the DB

### 0.4.0 (2020-04-27)
* Renamed the `DB` class to `JsonlDB`
* `open()` now skips empty lines
* `open()` throws an error with the line number when it encounters an invalid line. These errors can be ignored using the new constructor options argument.

### 0.3.0 (2020-04-26)
* Added `importJson` and `exportJson` methods

### 0.2.0 (2020-04-25)
* Added `isOpen` property

### 0.1.3 (2020-04-25)
* Writes that happen while `compress()` replaces files are now persisted

### 0.1.2 (2020-04-25)
* `compress()` no longer overwrites the main file while the DB is being closed

### 0.1.1 (2020-04-25)
* Fixed some race conditions

### 0.1.0 (2020-04-25)
First official release
