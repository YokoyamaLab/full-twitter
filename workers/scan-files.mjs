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
    const isTrash = await async.mapValues(workerData.filters, async (file) => {
        const { isTrash } = await import(file);
        return isTrash;
    });
    let reduceMemoDefault = {};
    const commands = await async.map(config.query, async (q) => {
        const { map, reduce, reduceMemo, tweetMask } = await import(addons[q.addon]);
        reduceMemoDefault[q.addon] = clone(reduceMemo);
        return {
            map: map,
            reduce: reduce,
            tweetMask: q.hasOwnProperty("mask") ? q.mask : tweetMask,
            filters: q.hasOwnProperty("filters") ? q.filters : null,
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
            reduceMemo[addonName] = clone(reduceMemoDefault[addonName]);
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
                try {
                    const tweet = JSON.parse(line);
                    const ts = DateTime.fromMillis(parseInt(tweet.timestamp_ms));
                    const tFrom = DateTime.fromMillis(config.from);
                    const tTo = DateTime.fromMillis(config.to);
                    if (tFrom <= ts && ts < tTo && config.hourWindow.indexOf(parseInt(ts.toFormat('H'))) !== -1) {
                        if (command.hasOwnProperty("filters") && command.filters != null) {
                            const filters = Array.isArray(command.filters) ? command.filters : [command.filters];
                            if (await async.detect(
                                filters,
                                async (filter) => {
                                    return isTrash[filter](tweet);
                                }
                            )) {
                                return null;
                            }
                        }
                        const { message, record } = await command.map(tweet, command.option, config);
                        await async.each(Array.isArray(message) ? message : [message], async (msg) => {
                            reduceMemo[command.addon] = await command.reduce(reduceMemo[command.addon], msg, config);
                            return null;
                        });
                        if (record === false) {
                            return null;
                        }
                        nHit++;
                        let maskedJson = jsonMask(tweet, command.tweetMask);
                        if (record !== true) {
                            maskedJson[command.addon] = record;
                        }
                        //console2.log(JSON.stringify(record, null, "\t"));
                        await tweetFile[command.addon].write(JSON.stringify(maskedJson) + "\n");
                    }
                } catch (e) {
                    console2.error("[JSON PARSER ERROR] ", filename);
                    console2.error(line);
                    console2.error("[Skip this tweet and Go!]");
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