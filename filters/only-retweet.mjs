/** 
 * full-twitter フィルター: ignore-retweet
 * コメント無しリツイートの排除
 *  
 * @author Shohei Yokoyama
 * @example <caption>Example usage of option.</caption>
 * 
 * "filter": ["ignore-retweet"],
 *      

 * @module filter/only-retweet.mjs 
 */

/**
 *  @type {string} - フィルタ名
 */
export const filterName = "only-retweet";

/**
* isTrash関数：無視すべきTweetかの判定
*
* @export
* @param {Object} tweet - JSON形式のTweet
* @param {*} config - コマンドライン引数で与えられるConfig
* @return {boolean} - 無視すべきならtrue、そうでないならfalse
*  */
export function isTrash(tweet, config) {
    return !tweet.hasOwnProperty("retweeted_status");
}