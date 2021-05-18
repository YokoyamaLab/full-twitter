import fs from 'fs';
import path from 'path';
import async from "async";
import readline from 'readline'
import clone from "rfdc/default";
import { Worker } from 'worker_threads';
import { Console } from 'console';
import { DateTime } from 'luxon';

const config_default = {
    query: [],
    input: '/data/local/twitter',
    output: './result',
    addons: './addons',
    nParallel: 40,
    nConcurrence: 4,
    nParallelDirScan: 11,
    nConcurrenceDirScan: 21,
    outputMaxLines: 1000000,
    resume: '.resume',
    preventResume: false,
    verbose: false,
    dayFrom: '2020-04-01',
    dayTo: '2021-03-31',
};

const workers = {
    'listFiles': path.resolve('./workers/list-files.mjs'),
    'scanFiles': path.resolve('./workers/scan-files.mjs'),
    'aggregateReduce': path.resolve('./workers/aggregate.mjs'),
    'concatenateTweets': path.resolve('./workers/concatenate.mjs')
};

const stats = {
    tweeta: 0,
    hita: 0
}
const console2 = new Console(process.stderr);

export async function FullTwitter(config_override) {
    try {
        var timeStart = DateTime.now();
        console2.time("STEPS");
        const config = await buildConfig(config_override);
        const { _scanAddon, _scanDirectory } = await async.parallel({
            _scanAddon: async () => await scanAddon(config),
            _scanDirectory: async () => await scanDirectory(config)
        });
        const { addonFiles } = _scanAddon;
        const { files } = _scanDirectory;
        const { targetFiles, resumeHandle } = await resume(config, files);
        const fileChunks = await fileChunker(config, targetFiles);
        await startWorker(config, addonFiles, fileChunks, resumeHandle);
        resumeHandle.close();

        console2.log("[SCAN DONE]");
        console2.timeLog("STEPS");
        await aggregateReduce(config);
        console2.log("[REDUCE DONE]");
        console2.timeLog("STEPS");
        await sortTweet(config);
        console2.log("[SORT DONE]");
        console2.timeLog("STEPS");
        await concatenateTweet(config);
        console2.log("[CONCAT DONE]");
        console2.timeLog("STEPS");
        await finalize(config, timeStart, DateTime.now());
        console2.log("[FINALIZE]");
        console2.timeEnd("STEPS");
    } catch (err) {
        console.log("ERROR");
        console.error(err);
    }
}
async function finalize(config, timeStart, timeEnd) {
    const resumeFile = path.join(config.output, ".resume");
    const rs = fs.createReadStream(resumeFile);
    const rl = readline.createInterface({ input: rs });
    let tsMax = 0;
    let tsMin = Number.MAX_VALUE;
    let stats = [];
    for await (const line of rl) {
        let [file, nHit, nTweet, tStart, tEnd] = line.split("\t");
        nHit = parseInt(nHit);
        nTweet = parseInt(nTweet);
        const tsStart = Math.floor(parseInt(tStart) / 1000);
        const tsEnd = Math.floor(parseInt(tEnd));
        tsMax = Math.max(tEnd, tsMax);
        tsMin = Math.min(tStart, tsMin)
        stats.push({ file, nTweet, tsStart, tsEnd });
    }
    const speedFile = await fs.promises.open(path.join(config.output, '_tweets-per-sec.stats'), 'w');
    for (let ts = tsMin; ts <= tsMax; ts++) {
        let procTweets = await async.reduce(stats, 0, (memo, stat) => {
            if (stat.tsStart < ts && ts <= stat.tsEnd) {
                memo += stat.nTweet / (stat.tsEnd - stat.tsStart);
            }
            return memo;
        });
        await speedFile.write(ts + "\t" + procTweets + "\n");
    }
    await speedFile.close();
    await fs.promises.writeFile(path.join(config.output, "_processing-time.stats"), JSON.stringify({
        "time in miliseconds": timeEnd.diff(timeStart),
        "time": timeEnd.diff(timeStart).toFormat("d Days hh Hours mm Minutes ss.S Seconds")
    }, null, "\t"));
    if (!config.verbose) {
        await fs.promises.rm(path.join(config.output, "reduce"), { recursive: true, force: true });
        await fs.promises.rm(path.join(config.output, "tweet"), { recursive: true, force: true });
        await fs.promises.unlink(path.join(config.output, ".resume"));
    }
}

async function concatenateTweet(config) {
    await async.each(config.query, async (q) => {
        const targetDirAddon = path.resolve(path.join(config.output, "tweet", q.addon));
        const sortedTweetFiles = await async.reduce(
            await fs.promises.readdir(targetDirAddon, { withFileTypes: true }),
            [],
            async (memo, dirent) => {
                const targetDirAddonDay = path.join(targetDirAddon, dirent.name);
                //console.log({ targetDirAddonDay });
                await async.each(
                    await fs.promises.readdir(targetDirAddonDay, { withFileTypes: true }),
                    async (direntt) => {
                        if (direntt.isFile() && direntt.name.match(/\.tweet\.sorted$/)) {
                            const targetFile = path.join(targetDirAddonDay, direntt.name);
                            //console.log({ targetFile });
                            memo.push(targetFile);
                        }
                        return null;
                    }
                );
                return memo;
            }
        );
        let counter = 0;
        let limit = config.outputMaxLines;
        let concatenatedTweetHandle = null;
        await async.eachSeries(sortedTweetFiles, async (tweetFile) => {
            const rs = fs.createReadStream(tweetFile);
            const rl = readline.createInterface({ input: rs });
            for await (const line of rl) {
                if (counter++ % limit == 0) {
                    if (concatenatedTweetHandle != null) {
                        await concatenatedTweetHandle.close();
                    }
                    concatenatedTweetHandle = await fs.promises.open(path.join(config.output, q.addon + ".tweet." + Math.floor(counter / limit)), "w");
                }
                concatenatedTweetHandle.write(line + "\n");
            }
        });
    });
}
async function sortTweet(config) {
    const tweetFiles = await async.reduce(config.query, [], async (memo, q) => {
        const targetDirAddon = path.resolve(path.join(config.output, "tweet", q.addon));
        //console.log({ targetDirAddon });
        await async.each(
            await fs.promises.readdir(targetDirAddon, { withFileTypes: true }),
            async (dirent) => {
                const targetDirAddonDay = path.join(targetDirAddon, dirent.name);
                //console.log({ targetDirAddonDay });
                await async.each(
                    await fs.promises.readdir(targetDirAddonDay, { withFileTypes: true }),
                    async (direntt) => {
                        if (!direntt.isFile() || !direntt.name.match(/\.tweet$/)) {
                            return null;
                        }
                        const targetFile = path.join(targetDirAddonDay, direntt.name)
                        memo.push(targetFile);
                        return null;
                    }
                );
            }
        );
        return memo;
    });
    await async.eachLimit(tweetFiles, config.nParallel, async (tweetFile) => {
        return new Promise((resolve, reject) => {
            const worker = new Worker("./workers/sort.mjs", {
                "workerData": {
                    'tweetFile': tweetFile,
                    'config': config
                }
            });
            worker.on('exit', async () => {
                resolve();
            });
        });
    });

}

async function aggregateReduce(config) {
    let dailyReduceSet = {};
    const outputDailyDirs = await async.reduce(config.query, [], async (memoDirs, q) => {
        const dirs = await async.reduce(
            await fs.promises.readdir(path.join(config.output, "reduce", q.addon), { withFileTypes: true }),
            [],
            async (memo, dirent) => {
                const reducePath = path.join(config.output, "reduce", q.addon, dirent.name);
                try {
                    const reduceFile = await fs.promises.open(path.join(reducePath, '.reduce'), 'wx');
                    reduceFile.close();
                } catch (e) {
                    //console.error(e);
                    console.error("[SKIP]", reducePath);
                    return memo;
                }
                if (!dailyReduceSet.hasOwnProperty(q.addon)) {
                    dailyReduceSet[q.addon] = [];
                }
                dailyReduceSet[q.addon].push(path.join(reducePath, ".reduce"));
                memo.push({
                    "addon": path.resolve(path.join(config.addons, q.addon + '.mjs')),
                    "dir": reducePath
                });
                return memo;
            }
        );
        return memoDirs.concat(dirs);
    });
    await async.eachLimit(outputDailyDirs, config.nParallel, async (outputDailyDir) => {
        return new Promise((resolve, reject) => {
            const worker = new Worker("./workers/aggregate.mjs", {
                "workerData": {
                    'targetDirectory': outputDailyDir.dir,
                    'addonPath': outputDailyDir.addon,
                    'config': config,
                }
            });
            worker.on('exit', async () => {
                resolve();
            });
        });
    });
    await async.eachOf(dailyReduceSet, async (dailyReduceFiles, addonName) => {
        const { reduceMemo, aggregate } = await import(path.resolve(path.join(config.addons, addonName + '.mjs')));
        const reduced = await async.reduce(dailyReduceFiles, clone(reduceMemo), async (memo, dailyReduceFile) => {
            const source = await fs.promises.readFile(dailyReduceFile);
            if (source.toString().trim() == "") {
                return memo;
            }
            return aggregate(memo, JSON.parse(source));
        });
        await fs.promises.writeFile(path.join(config.output, addonName + ".reduce"), JSON.stringify(reduced, null, "\t"));
    });
}

async function buildConfig(config_override) {
    console2.log('[init] Config');
    let config = {};
    await async.each(Object.keys(config_default), async (key) => {
        if (config_override.hasOwnProperty(key)) {
            config[key] = config_override[key];
        } else {
            config[key] = config_default[key];
        }
    });
    config.input = path.resolve(config.input);
    config.output = path.resolve(config.output);
    config.addons = path.resolve(config.addons);
    config.resume = path.resolve(path.join(config.output, config.resume));
    const tFrom = DateTime.fromISO(config.dayFrom);
    const tTo = DateTime.fromISO(config.dayTo);
    if (config.dayTo.split('T').length == 1) {
        tTo.plus({ days: 1 });
    }
    config.from = tFrom.toMillis();
    config.to = tTo.toMillis();
    config.dayFrom = tFrom.setZone("utc").toISO();
    config.dayTo = tTo.setZone("utc").toISO();
    console2.table(config);
    config.query = JSON.parse(await fs.promises.readFile(config.query));
    return config;
}

async function scanAddon(config) {
    let addons = await async.reduce(
        await fs.promises.readdir(config.addons, { withFileTypes: true }),
        {
            'addonResult': {},
            'addonFiles': {}
        },
        async (memo, dirent) => {
            if (dirent.isFile() && dirent.name.match(/\.mjs$/)) {
                try {
                    const { addonName, result } = await import(path.resolve(path.join(config.addons, dirent.name)));
                    memo.addonResult[addonName] = result;
                    memo.addonFiles[addonName] = path.resolve(path.join(config.addons, dirent.name));
                } catch (e) {
                    console2.error("AddOn Load ERROR:", dirent.name);
                    console2.error(e);
                    return memo;
                };
            }
            return memo;
        }
    );
    console2.log("[init] Load AddOns:", Object.keys(addons.addonFiles));
    return addons;
}

async function scanDirectory(config) {
    const directories = await async.reduce(
        await fs.promises.readdir(config.input, { withFileTypes: true }),
        [],
        async (memo, dirent) => {
            if ((dirent.isDirectory() || dirent.isSymbolicLink()) && dirent.name.match(/^\d{4}-\d{2}-\d{2}$/)) {
                const ts_min = DateTime.fromISO(dirent.name + "T00:00:00");
                const ts_max = DateTime.fromISO(dirent.name + "T23:59:59");
                if (config.from <= ts_max.toMillis() && ts_min.toMillis() <= config.to) {
                    //console2.log("[watch]", dirent.name);
                    memo.push(path.join(config.input, dirent.name));
                }
            }
            return memo;
        });
    let directoryChunks = await async.reduce(directories,
        [[]],
        async (memo, directory) => {
            if (memo[memo.length - 1].length >= config.nConcurrenceDirScan) {
                memo.push([]);
            }
            memo[memo.length - 1].push(directory);
            return memo;
        });
    let nFile = 0;
    let files = [];
    await async.eachOfLimit(directoryChunks, config.nParallelDirScan, async (directoryChunk) => {
        return new Promise((resolve, reject) => {
            const worker = new Worker(workers.listFiles, { workerData: { 'directoryChunk': directoryChunk } });
            worker.on('message', (message) => {
                files = files.concat(message);
                nFile += message.length;
            });
            worker.on('exit', () => {
                resolve();
            });
        });
    });
    console2.log("[init] Scan Tweet Files");
    console2.table({
        'Directories': directories.length,
        'Files': nFile
    });
    return { directories, files };
}

async function fileChunker(config, files) {
    console2.log("[init] Make chunks from ", files.length, "files");
    console2.group();
    let fileChunks = [];
    await async.eachOf(files, async (file, idx) => {
        const chunkId = Math.floor(idx / config.nConcurrence);
        if (typeof fileChunks[chunkId] === 'undefined') {
            fileChunks[chunkId] = [];
        }
        fileChunks[chunkId].push(file);
        return null;
    });
    console2.log("Total", fileChunks.length, "chunks");
    console2.groupEnd();
    return fileChunks;
}

async function resume(config, all_files) {
    try {
        await fs.promises.stat(config.resume);
        if (config.preventResume) {
            throw "NO-RESUME-FILE";
        }
        console2.log("[RESUME YES]", "Resume previous session");
        console2.group();
        let doneFiles = {};
        for await (const line of readline.createInterface({ input: fs.createReadStream(config.resume) })) {
            const fields = line.split("\t");
            doneFiles[fields[0]] = true;
        }
        const targetFiles = await async.filter(all_files, async (file) => {
            return !doneFiles.hasOwnProperty(file);
        });
        const resumeHandle = await fs.promises.open(config.resume, "w+");
        console2.log(files.length, "files remain");
        console2.groupEnd();
        return { targetFiles, resumeHandle };
    } catch (e) {
        console2.log("[RESUME NO]", "Start new session");
        await fs.promises.rm(config.output, { recursive: true, force: true });
        await fs.promises.mkdir(config.output);
        await fs.promises.writeFile(path.join(config.output, "_config.json"), JSON.stringify(config, null, "\t"));
        const resumeHandle = await fs.promises.open(config.resume, "w+");
        const targetFiles = all_files;
        return { targetFiles, resumeHandle };
    }
}

async function startWorker(config, addonFiles, fileChunks, resumeHandle) {
    console2.log("[start]:", config.nParallel, "parallel");
    await async.eachLimit(fileChunks, config.nParallel, async (fileChunk) => {
        return new Promise((resolve, reject) => {
            const worker = new Worker(workers.scanFiles, { workerData: { 'files': fileChunk, 'config': config, 'addons': addonFiles } });
            worker.on('message', async (message) => {
                switch (message.type) {
                    case 'message':
                        break;
                    case 'done':
                        const filename = path.basename(message.file, '.lz4');
                        console2.log("[done]", filename, Math.round(100000 * message.nTweet / (message.tEnd - message.tStart)) / 100, "tps", message.nHit, "hits");
                        await resumeHandle.write(filename + "\t" + message.nHit + "\t" + message.nTweet + "\t" + message.tStart + '\t' + message.tEnd + '\n');
                        stats.tweets += message.nTweet;
                        stats.hits += message.nHit
                        break;
                    case 'exit':
                        break;
                }
            });
            worker.on('exit', async () => {
                resolve();
            });
        });
    });
}