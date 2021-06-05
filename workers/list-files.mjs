import { workerData, parentPort } from 'worker_threads'
import fs from 'fs';
import path from 'path';
import async from "async";
import { Console } from 'console';
import { DateTime } from 'luxon';

const console2 = new Console(process.stderr);
const config = workerData.config;
const isTargetFile = (fileName) => {
    const [year, month, day, hour, minute, no] = path.basename(fileName, '.lz4').split("-", 6);
    const fileTime = DateTime.utc(parseInt(year), parseInt(month), parseInt(day), parseInt(hour), parseInt(minute));
    const fileHour = fileTime.hour;
    const tFrom = DateTime.fromMillis(config.from);
    const tTo = DateTime.fromMillis(config.to).plus({ minutes: 15 });
    /*
    if (config.hourWindow.indexOf(fileHour) !== 0) {
        console2.log("[[ OutOfWindow ]]", fileName);
    }
    if ((tFrom > fileTime) || (fileTime > tTo)) {
        console2.log("<< OutOfTerm >>", fileName);
    }
    if ((config.hourWindow.indexOf(fileHour) !== -1) && (tFrom <= fileTime) && (fileTime <= tTo)) {
        console2.log(config.hourWindow,">",fileHour,">>", fileName);
    }
    */
    return (config.hourWindow.indexOf(fileHour) !== -1) && (tFrom <= fileTime) && (fileTime <= tTo);
}

await parentPort.postMessage(
    await async.reduce(workerData.directoryChunk, [], async (memo, directory) => {
        const files = await async.map(
            await async.filter(
                await fs.promises.readdir(directory, { withFileTypes: true }),
                async (dirent) => {
                    return dirent.isFile() && isTargetFile(dirent.name);
                }
            ),
            async (dirent) => {
                return path.join(directory, dirent.name);
            }
        )
        return memo.concat(files);
    })
);