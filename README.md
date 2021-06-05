# Twitter全量解析用ファイルスキャンツール

## はじめに

このプログラムはソーシャルデータがJSON形式で1投稿1行で保存された大規模ファイル群に対してフルスキャンを行いMap/Reduce/Aggregate処理を行います。

### 前提となるディレクトリ構成

```
root/
　├ 2020-04-01/
　│　├ 2020-04-01-00-00-00.lz4
　│　├ 2020-04-01-00-10-00.lz4 
　│　├ 2020-04-01-00-10-01.lz4 
　│　⋮ 
　│　├ 2020-04-01-23-40-00.lz4
　│　└ 2020-04-01-23-50-00.lz4
　├ 2020-04-02/
　│　├ 2020-04-02-00-00-00.lz4
　│　├ 2020-04-02-00-10-00.lz4 
　│　├ 2020-04-02-00-20-01.lz4 
　│　⋮ 
　│　├ 2020-04-02-23-50-00.lz4
　│　└ 2020-04-02-23-50-01.lz4
　⋮
　└ 2021-03-31/
　 　├ 2021-03-31-00-00-00.lz4
　 　├ 2021-03-31-00-10-00.lz4 
　 　├ 2021-03-31-00-10-01.lz4 
　 　⋮ 
　 　├ 2021-03-31-23-40-00.lz4
　 　└ 2021-03-31-23-50-00.lz4
```

* **ルール**
    * ファイルは日別にディレクトリに分割して格納されている
      * 例：2020-04-01
      * 構成：yyyy-mm-dd
    * 各日付ディレクトリの中にlz4形式で圧縮されたファイルが以下の命名規則で保存されている
        * 例：2020-04-01-00-10-01.lz4 
        * 構成：yyyy-mm-dd-hh-mm-nn.lz4
            *  yyyy: 西暦
            *  mm: 月
            *  dd: 日
            *  mm: 分(10分単位Widnow)
            *  nn: 連番(その10分Windowでのデータ量が多い場合は連番を降ってファイルを分割している)
   * 各ファイルを解凍すると、一投稿が1行のJSONとなって格納されている。


## 利用方法

### コマンドラインオプション

```ShellSession
$ node bin.mjs -?
Usage: node bin.mjs [options]

Options:
  -q, --query <path>              クエリJSONファイル (default: "./query.json")
  -i, --input <path>              Twiiterアーカイブディレクトリ名 (default: "/data/local/twitter")
  -o, --output <path>             結果出力ディレクトリ名 (default: "result")
  -m, --outputMaxLines <integer>  結果ファイル一つ当たりの行数 (default: 1000000)
  -p, --n-parallel <integer>      並列実行スレッド数 (default: 45)
  -c, --n-concurrence <integer>   スレッド毎の平行処理ファイル数 (default: 4)
  -n, --prevent-resume            レジュームをせず新しいセッションを実行する
  -r, --resume <path>             レジューム用作業記録ファイル (default: ".resume")
  -v, --verbose                   途中作成ファイルを全て残す
  -f, --day-from <yyyy-mm-dd>     処理対象期間(開始)ISO8601形式 (default: "2020-04-01T00:00:00")
  -t, --day-to <yyyy-mm-dd>       処理対象期間(終了)ISO8601形式 (default: "2021-03-31T23:59:59")
  -h, --hour-window <[h,h,...]>   収集対象時間帯
  -?, --help                      コマンドの使用方法表示
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
