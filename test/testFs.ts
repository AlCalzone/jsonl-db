import os from "os";
import path from "path";
import { vi } from "vitest";

/** Class to manage an isolated test "filesystem" for unit tests */
export class TestFS {
	private _fs: undefined | typeof import("fs-extra");
	private async fs(): Promise<typeof import("fs-extra")> {
		if (!this._fs) {
			this._fs = await vi.importActual("fs-extra");
		}
		return this._fs!;
	}

	private testFsRoot: string | undefined;
	async getRoot(): Promise<string> {
		if (!this.testFsRoot) {
			const fs = await this.fs();
			this.testFsRoot = await fs.mkdtemp(
				`${os.tmpdir()}${path.sep}jsonl-db-test-`,
			);
		}
		return this.testFsRoot;
	}

	private normalizePath(testRoot: string, filename: string): string {
		const relativeToFsRoot = path.relative(
			"/",
			path.resolve("/", filename),
		);
		return path.resolve(testRoot, relativeToFsRoot);
	}

	/** Creates a test directory and file structure with the given contents */
	async create(structure: Record<string, string | null> = {}): Promise<void> {
		const root = await this.getRoot();
		const fs = await this.fs();
		await fs.emptyDir(root);
		for (const [filename, content] of Object.entries(structure)) {
			const normalizedFilename = this.normalizePath(root, filename);
			if (content === null) {
				// this is a directory
				await fs.ensureDir(normalizedFilename);
			} else {
				// this is a file
				await fs.ensureDir(path.dirname(normalizedFilename));
				await fs.writeFile(normalizedFilename, content, "utf8");
			}
		}
	}

	/** Removes the test directory structure */
	async remove(): Promise<void> {
		if (!this.testFsRoot) return;
		const fs = await this.fs();
		await fs.remove(this.testFsRoot);
	}
}
