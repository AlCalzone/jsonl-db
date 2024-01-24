import {
	createDeferredPromise,
	type DeferredPromise,
} from "alcalzone-shared/deferred-promise";

/** Can be used to asynchronously signal a single listener */
export class Signal {
	private _listener: DeferredPromise<void> | undefined;

	private _status: boolean = false;
	public get isSet(): boolean {
		return this._status;
	}

	public set(): void {
		if (this._status) return;
		this._status = true;
		if (this._listener) {
			this._listener.resolve(undefined);
		}
	}

	public reset(): void {
		this._status = false;
		this._listener = undefined;
	}

	public then<T>(onfulfilled: () => T | PromiseLike<T>): Promise<T> {
		if (this._status) {
			return Promise.resolve(onfulfilled());
		} else {
			const p = createDeferredPromise();
			this._listener = p;
			return p.then(onfulfilled);
		}
	}
}
