# Twitter全量解析用ファイルスキャンツール

## 利用方法

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
