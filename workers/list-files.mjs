import {  workerData, parentPort } from 'worker_threads'
import fs from 'fs';
import path from 'path';
import async from "async";

await parentPort.postMessage(
    await async.reduce(workerData.directoryChunk, [], async (memo, directory) => {
        const files = await async.map(
            await async.filter(
                await fs.promises.readdir(directory, { withFileTypes: true }),
                async (dirent) => {
                    return dirent.isFile();
                }
            ),
            async (dirent) => {
                return path.join(directory, dirent.name);
            }
        )
        return memo.concat(files);
    })
);