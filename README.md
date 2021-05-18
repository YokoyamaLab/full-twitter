# Twitter全量解析用ファイルスキャンツール

## 利用方法

### コマンドラインオプション

```ShellSession
$ node bin.mjs -h
Usage: node bin.mjs [options]

Options:
  -q, --query <path>              クエリJSONファイル (default: "./query.json")
  -i, --input <path>              Twiiterアーカイブディレクトリ名 (default:"/data/local/twitter")
  -o, --output <path>             結果出力ディレクトリ名 (default: "result")
  -m, --outputMaxLines <integer>  結果ファイル一つ当たりの行数 (default: 1000000)
  -p, --n-parallel <integer>      並列実行スレッド数 (default: 45)
  -c, --n-concurrence <integer>   スレッド毎の平行処理ファイル数 (default: 4)
  -n, --prevent-resume            レジュームをせず新しいセッションを実行する
  -r, --resume <path>             レジューム用作業記録ファイル (default: ".resume")
  -v, --verbose                   途中作成ファイルを全て残す
  -f, --day-from <yyyy-mm-dd>     処理対象期間(開始)ISO8601形式 (default:"2020-04-01T00:00:00")
  -t, --day-to <yyyy-mm-dd>       処理対象期間(終了)ISO8601形式 (default:"2021-03-31T23:59:59")
  -h, --help                      display help for command
```

### search-keywordアドオン

#### ユースケース１
* **あけおめ**あるいは**初詣**と言及しているツイートを抽出する
    * あけおめ、あけましておめでとう、明けましておめでとう、happy new yearは同一視する。
* 期間は2020年12月30日～2021年01月03日
* 問い合わせファイル(queries/akeome.json)

```JSON
[
    {
        "addon": "keyword-search",
        "option": {
            "keyword": [["あけおめ","あけましておめでとう","明けましておめでとう","happy new year"],"初詣"]
        }
    }
]
```

* コマンド

```Shell
nohup node bin.mjs -p 45 -c 4 -f 2020-12-30 -t 2021-01-03 -q queries/akeome.json -o results/akeome -n > .nohup-out 2> .nohup-error &
```
