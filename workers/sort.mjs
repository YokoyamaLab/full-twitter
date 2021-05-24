import { workerData, parentPort } from 'worker_threads'
import fs from 'fs';
import readline from 'readline'
import async from "async";
import * as TimSort from 'timsort';
import { Console } from 'console';
const console2 = new Console(process.stderr);

const tweetFile = workerData.tweetFile;
const sortedTweetFile = workerData.tweetFile + ".sorted";
const config = workerData.config;

const rs = fs.createReadStream(tweetFile);
//console2.log(tweetFile);
let tweets = [];
const rl = readline.createInterface({ input: rs });
for await (const line of rl) {
    try {
        tweets.push(JSON.parse(line));
    } catch (e) {
        console2.error("JSON Parse ERROR at sort.mjs");
        console2.error(line);
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
    } catch (e) {
        console2.error("SORT [SKIP]", sortedTweetFile);
        console2.error(e);
    }
} else {
    console2.error("SORT [ZERO]", sortedTweetFile);
}