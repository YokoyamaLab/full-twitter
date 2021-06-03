import { workerData, parentPort, threadId } from 'worker_threads'
import fs from 'fs';
import path from 'path';
import async from "async";
import lz4 from 'lz4'
import readline from 'readline'
import jsonMask from 'json-mask';
import { Console } from 'console';
import { DateTime } from 'luxon';
import clone from "rfdc/default";

const console2 = new Console(process.stderr);

const files = workerData.files;
const config = workerData.config;
const addons = workerData.addons;

try {
    //console2.log("[" + threadId + "]", "Start worker");
    let redumeMemoDefault = {};
    const commands = await async.map(config.query, async (q) => {
        const { map, reduce, reduceMemo, tweetMask } = await import(addons[q.addon]);
        redumeMemoDefault[q.addon] = clone(reduceMemo);
        return {
            map: map,
            reduce: reduce,
            tweetMask: tweetMask,
            addon: q.addon,
            option: q.option
        };
    });

    await async.each(files, async (file) => {
        const subdir = path.dirname(file).split(path.sep).pop();
        const filename = path.basename(file, '.lz4');
        //console.log("[load]", filename);
        let reduceFile = {};
        let reduceMemo = {};
        let tweetFile = {};
        await async.eachOf(addons, async (addon, addonName) => {
            reduceFile[addonName] = path.join(config.output, "reduce", addonName, subdir, filename + ".reduce");
            tweetFile[addonName] = path.join(config.output, "tweet", addonName, subdir, filename + ".tweet");
            reduceMemo[addonName] = clone(redumeMemoDefault[addonName]);
            //console2.log("[OUTPUT FILE]", config.output, "tweet", addonName, subdir, filename + ".tweet");
            try {
                await fs.promises.mkdir(path.join(config.output, "reduce", addonName, subdir), { recursive: true });
            } catch (e) {
                if (!e || (e && e.code === 'EEXIST')) { } else { throw e; }
            }
            try {
                await fs.promises.mkdir(path.join(config.output, "tweet", addonName, subdir), { recursive: true });
            } catch (e) {
                if (!e || (e && e.code === 'EEXIST')) { } else { throw e; }
            }
            tweetFile[addonName] = await fs.promises.open(tweetFile[addonName], 'w');
        });
        let nTweet = 0;
        let nHit = 0;
        let tStart = new Date().getTime();
        const decoder = lz4.createDecoderStream();
        const input = fs.createReadStream(file);
        const reader = readline.createInterface({ input: input.pipe(decoder), crlfDelay: Infinity });
        for await (const line of reader) {
            nTweet++;
            await async.each(commands, async (command) => {
                if (line.trim() == "") {
                    return null;
                }
                const tweet = JSON.parse(line);
                const ts = DateTime.fromMillis(parseInt(tweet.timestamp_ms));
                const tFrom = DateTime.fromMillis(config.from);
                const tTo = DateTime.fromMillis(config.to);
                if (tFrom <= ts && ts < tTo) {
                    const { message, record } = await command.map(tweet, command.option, config);
                    await async.each(Array.isArray(message) ? message : [message], async (msg) => {
                        reduceMemo[command.addon] = await command.reduce(reduceMemo[command.addon], msg, config);
                        return null;
                    });
                    if (record === false) {
                        return null;
                    }
                    const maskedJson = jsonMask(tweet, command.tweetMask);
                    nHit++;
                    if (record !== true) {
                        maskedJson[command.addon] = record;
                    }
                    await tweetFile[command.addon].write(JSON.stringify(maskedJson) + "\n");

                }
                return null;
            });
        }
        const tEnd = new Date().getTime();
        await async.each(commands, async (command) => {
            tweetFile[command.addon].close();
            const rFile = await fs.promises.open(reduceFile[command.addon], 'w');
            await rFile.write(JSON.stringify(reduceMemo[command.addon]) + "\n");
            await rFile.close();
            return null;
        });
        await parentPort.postMessage({
            type: "done",
            file: file,
            nTweet: nTweet,
            nHit: nHit,
            tStart: tStart,
            tEnd: tEnd,
            threadId: threadId
        });
    });
} catch (eee) {
    console2.log(eee);
}