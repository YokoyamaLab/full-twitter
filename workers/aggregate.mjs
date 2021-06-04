import { workerData, parentPort } from 'worker_threads'
import fs from 'fs';
import path from 'path';
import async from "async";
import clone from "rfdc/default";
import { Console } from 'console';
import * as TimSort from 'timsort';

const { reduceMemo, aggregate } = await import(workerData.addonPath);
const targetDirectory = workerData.targetDirectory;
const config = workerData.config;
const console2 = new Console(process.stderr);

const files = await async.map(await async.filter(
    await fs.promises.readdir(targetDirectory, { withFileTypes: true }),
    async (dirent) => {
        return dirent.isFile() && dirent.name.match(/\.reduce$/);
    }
), async (dirent) => {
    return path.resolve(path.join(targetDirectory, dirent.name))
});

let doneReduceFiles = [];
const reduced = await async.reduce(files, clone(reduceMemo), async (memo, file) => {
    const source = await fs.promises.readFile(file);
    doneReduceFiles.push(file);
    if (source.toString().trim() == "") {
        return memo;
    }
    return aggregate(memo, JSON.parse(source));
});
if (!config.verbose) {
    await async.each(doneReduceFiles, async (file) => {
        try {
            await fs.promises.unlink(file);
        } catch (e) {
            console2.error("[FAIL at reduce] delete->", file);
        }
    });
}
console2.log("\n[aggregate]",targetDirectory);
console2.log( JSON.stringify(reduced, null, "\t"));
await fs.promises.writeFile(path.join(targetDirectory, ".reduce"), JSON.stringify(reduced, null, "\t"));

