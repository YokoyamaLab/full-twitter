import { workerData, parentPort } from 'worker_threads'
import fs from 'fs';
import path from 'path';
import readline from 'readline'
import async from "async";
import clone from "rfdc/default";
import * as TimSort from 'timsort';

const tweetFile = workerData.tweetFile;
const sortedTweetFile = workerData.tweetFile + ".sorted";
const config = workerData.config;

const rs = fs.createReadStream(tweetFile);
//console.error(tweetFile);
let tweets = [];
const rl = readline.createInterface({ input: rs });
for await (const line of rl) {
    try {
        tweets.push(JSON.parse(line));
    } catch (e) {
        console.error("JSON Parse ERROR at sort.mjs");
        console.error(line);
        continue;
    }
}
if (tweets.length > 0) {
    try {
        const sortedTweetHandle = await fs.promises.open(sortedTweetFile, "wx");
        TimSort.sort(tweets, (a, b) => {
            return a["timestamp_ms"] - b["timestamp_ms"];
        });
        await async.eachSeries(tweets, async (tweet) => {
            sortedTweetHandle.write(JSON.stringify(tweet) + "\n");
            return null;
        });
        sortedTweetHandle.close();
        rs.close();
        if (!config.verbose) {
            fs.promises.unlink(tweetFile);
        }
    } catch (e) {
        //console.error("[SKIP]", sortedTweetFile);
        console.error(e);
    }
} else {
    //console.error("[zero]", sortedTweetFile);
    rs.close();
    if (!config.verbose) {
        fs.promises.unlink(tweetFile);
    }
}