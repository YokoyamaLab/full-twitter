import { FullTwitter } from './index.mjs';
import { Command } from 'commander/esm.mjs';
const program = new Command();

program
    .name("node bin.mjs")
    .option('-q, --query <path>', 'クエリJSONファイル','./query.json')
    .option('-o, --output <path>', '結果出力ディレクトリ名','result')
    .option('-m, --outputMaxLines <integer>', '結果ファイル一つ当たりの行数', 1000000)
    .option('-p, --n-parallel <integer>', '並列実行スレッド数', 45)
    .option('-c, --n-concurrence <integer>', 'スレッド毎の平行処理ファイル数', 4)
    .option('-n, --prevent-resume', 'レジュームをせず新しいセッションを実行する')
    .option('-r, --resume <path>', 'レジューム用作業記録ファイル', '.resume')
    .option('-v, --verbose','途中作成ファイルを全て残す')
    .option('-f, --day-from <yyyy-mm-dd>', '処理対象期間(開始)ISO8601形式', '2020-04-01T00:00:00')
    .option('-t, --day-to <yyyy-mm-dd>', '処理対象期間(終了)ISO8601形式', '2021-03-31T23:59:59');

program.parse(process.argv);
let config = program.opts();

try {

    console.warn("[START]");
    console.time('execution time');

    process.on('SIGTERM', async () => {
        process.exit(1);
    });
    process.on('SIGINT', async () => {
        process.exit(1);
    });

    const fullTwitter = await FullTwitter(config);

    console.log('/*');
    console.timeEnd('execution time');
    console.log('*/')
    console.warn("[ALL DONE]");


} catch (e) { console.error(e); }