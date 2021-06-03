/** 
 * full-twitter アドオン: keyword-search
 * キーワード検索と時系列出現頻度統計情報
 *  input: キーワード配列
 *  summary: キーワード、日にち、時間毎のキーワード出現数
 *  output: キーワードが含まれたTweet
 * @author Shohei Yokoyama
 * @example <caption>Example usage of option.</caption>
 * {
 *     "addon": "keyword-count",
 *      "option": {
 *          "keyword": ["ハチ公", "センター街", "竹下通り", "アメ横", "雷門", "歌舞伎町", "みなとみらい"],
 *     }
 *  }
 * @module addon/keyword-count.mjs 
 */

import async from "async";
import { DateTime } from 'luxon';
//import { Console } from 'console';
/**
 *  @type {string} - アドオン名
 */
export const addonName = "keyword-search";

/**
 *  Tweet JSONから保存するキーの選択
 *  文法はjson-maskモジュールに準ずる
 *  @see {@link https://www.npmjs.com/package/json-mask}
 */
export const tweetMask = 'id_str,text,user(id_str,name,screen_name),retweeted_status.id_str,entities(hashtags,urls),lang,timestamp_ms,created_at';


/** 
 * Reduce関数へ渡すMemoの初期値
 * @type {*} 
 * */
export const reduceMemo = {
    "count": {},
    "histogram": {}
};


/**
* aggregate関数：2つのreduce結果を併合一つのreduce結果にする
*
* @export
* @async
* @param {Onject} aReduceMemo - reduce関数の結果
* @param {Onject} bReduceMemo - reduce関数の結果
* @param {*} config - コマンドライン引数で与えられるConfig
*  */
export async function aggregate(aReduceMemo, bReduceMemo, config) {
    //const console2 = new Console(process.stderr);
    let a = aReduceMemo.histogram;
    let b = bReduceMemo.histogram;
    //aReduceMemo.count = aReduceMemo.count + bReduceMemo.count;
    //console2.log({ aReduceMemo, bReduceMemo });
    async.eachOf(b, async (bv, bKeyword) => {
        if (!a.hasOwnProperty(bKeyword)) {
            a[bKeyword] = {};
        }
        if (!aReduceMemo.count.hasOwnProperty(bKeyword)) {
            aReduceMemo.count[bKeyword] = 0;
        }
        aReduceMemo.count[bKeyword] += bReduceMemo.count[bKeyword];
        async.eachOf(bv, async (bvv, bDay) => {
            if (!a[bKeyword].hasOwnProperty(bDay)) {
                a[bKeyword][bDay] = {};
            }
            async.eachOf(bvv, async (n, bHour) => {
                if (!a[bKeyword][bDay].hasOwnProperty(bHour)) {
                    a[bKeyword][bDay][bHour] = 0;
                }
                a[bKeyword][bDay][bHour] += n;
                return null;
            });
            return null;
        });
        return null;
    });
    return aReduceMemo;
}

/**
* reduce関数：message毎に呼ばれる関数、結果の集約に利用。
*
* @export
* @async
* @param {Onject} memo - reduce関数の共通記憶領域
* @param {Onject} message - map関数で返されるmessage
* @param {*} config - コマンドライン引数で与えられるConfig
*  */
export async function reduce(memo, message, config) {
    if (!memo.histogram.hasOwnProperty(message.keyword)) {
        memo.histogram[message.keyword] = {};
    }
    if (!memo.histogram[message.keyword].hasOwnProperty(message.day)) {
        memo.histogram[message.keyword][message.day] = {};
    }
    if (!memo.histogram[message.keyword][message.day].hasOwnProperty(message.hour)) {
        memo.histogram[message.keyword][message.day][message.hour] = 0;
    }
    memo.histogram[message.keyword][message.day][message.hour]++;
    if (!memo.count.hasOwnProperty(message.keyword)) {
        memo.count[message.keyword] = 0;
    }
    memo.count[message.keyword]++;
    return memo;
}

/**
 * map関数：ツイート毎に呼ばれる関数（Workerスレッドで呼ばれる事に注意）
 *
 * @export
 * @async
 * @param {*} tweet - １ツイート分のJSON(Parse済み)
 * @param {*} option - ユーザによって与えられたQueryにあるCommand Option
 * @param {*} config - コマンドライン引数で与えられるConfig
 * @return {AddonMapReturn}
 */
export async function map(tweet, option, config) {
    let rec = [];
    const message = await async.reduce(option.keyword, [], async (memo, keyword) => {
        const keywords = Array.isArray(keyword) ? keyword : [keyword];
        const keykeyword = keywords[0];
        if (await async.detect(keywords, async (keyword) => {
            if ((option.hasOwnProperty("retweeted") && option.retweeted === true && tweet.retweeted === false)) {
                return false;
            }
            if ((option.hasOwnProperty("in_reply") && option.in_reply !== true && tweet.in_reply_to_status_id === null)) {
                return false;
            }
            return tweet.text.toUpperCase().indexOf(keyword.toUpperCase()) > -1;
        })) {
            const ts = DateTime.fromMillis(parseInt(tweet.timestamp_ms));
            rec.push(keykeyword);
            memo.push({
                keyword: keykeyword,
                day: ts.toISODate(),
                hour: ts.toFormat('H')
            });
        }
        return memo;
    });
    return {
        message: message,
        record: (rec.length == 0) ? false : rec
    }
}

/**
 * 走査終了時に結果(サマリ)を得る
 *
 * @export
 * @async
 * @return {*} - キーワード毎の日付・時間別ツイート数
 */
export async function result() {
    return count;
};

/**
 * Map関数の返り値
 * @typedef {Object} AddonMapReturn
 * @property {Message} message - Ruduce関数へ渡すためのmessage
 * @property {boolean|Object} record - そのTweetを記録するかのフラグ、あるいは記録の際に一緒に記録するObject形式のデータ(_fulltwitter属性に保存される)
 */


/**
 * Map関数(ワーカースレッド)からReduce関数(メインスレッド)へ送られるObject形式(JSON)のメッセーじ
 * @typedef {Object} Message
 * @property {Array|Object} message - 配列の場合は個々の要素が別々のmessageとしてreduceに渡される。空配列かnullを返すとmessageは送られない。
 */