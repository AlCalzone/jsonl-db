# jsonl-db

Simple JSONL-based key-value store. Uses an append-only file to store the data. With support for database dumps and compressing the db file.

![Build Status](https://action-badges.now.sh/AlCalzone/jsonl-db)
[![Coverage Status](https://img.shields.io/coveralls/github/AlCalzone/jsonl-db.svg)](https://coveralls.io/github/AlCalzone/jsonl-db)
[![Language grade: JavaScript](https://img.shields.io/lgtm/grade/javascript/g/AlCalzone/jsonl-db.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/AlCalzone/jsonl-db/context:javascript)
[![node](https://img.shields.io/node/v/@alcalzone/jsonl-db.svg) ![npm](https://img.shields.io/npm/v/@alcalzone/jsonl-db.svg)](https://www.npmjs.com/package/@alcalzone/jsonl-db)


## Usage

Load the module:

```ts
import { DB } from "@alcalzone/jsonl-db";
```

Open or create a database file:

```ts
const db = new DB("/path/to/file");
await db.open();
```
Now, `db.isOpen` is `true`.

Use the database like you would use a [`Map`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map).

The data is persisted asynchronously, so make sure to `close()` the DB when you no longer need it:

```ts
await db.close();
```
Now, `db.isOpen` is `false`. While the db is not open, any calls that access the data will throw an error.

To create a compressed copy of the database in `/path/to/file.dump`, use the `dump()` method. If any data is written to the db during the dump, it is appended to the dump but most likely compressed.

```ts
await db.dump();
```

After a while, the main db file may contain unnecessary entries. To remove them, use the `compress()` method.

```ts
await db.compress();
```

**Note:** During this call, `/path/to/file.dump` is overwritten and then renamed, `/path/to/file.bak` is overwritten and then deleted. So make sure you don't have any important data in these files.

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

### __WORK IN PROGRESS__
* Renamed the `DB` class to `JsonlDB`

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
