/** 
 * full-twitter フィルター: only-reply
 * リプライツイートのみ
 *  
 * @author Shohei Yokoyama
 * @example <caption>Example usage of option.</caption>
 * 
 * "filter": ["only-reply"],
 *      

 * @module filter/only-reply.mjs 
 */

/**
 *  @type {string} - フィルタ名
 */
export const filterName = "only-reply";

/**
* isTrash関数：無視すべきTweetかの判定
*
* @export
* @param {Object} tweet - JSON形式のTweet
* @param {*} config - コマンドライン引数で与えられるConfig
* @return {boolean} - 無視すべきならtrue、そうでないならfalse
*  */
export function isTrash(tweet, config) {
    return tweet.in_reply_to_status_id_str === null;
}