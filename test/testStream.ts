import { Stream } from "stream";

(async () => {
	const str = new Stream.PassThrough({ objectMode: true });
	process.nextTick(() => {
		str.write({ v: 1 });
		str.write({ v: 2 });
		setTimeout(() => {
			str.write({ v: 3 });
			str.end();
		}, 1000);
	});
	let chunk = [];
	for await (const item of str) {
		chunk.push(item);
		if (str.readableLength === 0) {
			chunk.forEach((i) => console.log(i));
			chunk = [];
		}
	}
	console.log("ended");
})();
