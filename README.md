# pybotters-inago-flyer
pybotters + asyncioを使った[inago flyer](https://inagoflyer.appspot.com/btcmac)ボットです。

## 使い方
依存ライブラリのインストール

```bash
pip install -r requirements.txt
```

API keyの入ったJSONファイル

```
# api.json
{
  "bitflyer": [
      "...",  # API Key
      "..."   # API Secrete
  ]
}
```

実行

```bash
python main.py --api_key_json PATH/TO/api.json
```

オプション

```bash
usage: main.py [-h] --api_key API_KEY [--symbol SYMBOL] [--side {BUY,SELL,BOTH}] [--size SIZE] [--bar_unit_seconds_long BAR_UNIT_SECONDS_LONG]
               [--bar_unit_seconds_short BAR_UNIT_SECONDS_SHORT] [--bar_maxlen BAR_MAXLEN] [--lower_threshold LOWER_THRESHOLD]
               [--upper_threshold UPPER_THRESHOLD] [--entry_patience_seconds ENTRY_PATIENCE_SECONDS] [--entry_price_change ENTRY_PRICE_CHANGE]
               [--trail_margin TRAIL_MARGIN]

options:
  -h, --help            show this help message and exit
  --api_key API_KEY     apiキーが入ったJSONファイル
  --symbol SYMBOL       取引通過
  --side {BUY,SELL,BOTH}
                        エントリーサイド
  --size SIZE           注文サイズ
  --bar_unit_seconds_long BAR_UNIT_SECONDS_LONG
                        長期足
  --bar_unit_seconds_short BAR_UNIT_SECONDS_SHORT
                        短期足
  --bar_maxlen BAR_MAXLEN
                        足の最大履歴
  --lower_threshold LOWER_THRESHOLD
                        短期足のボリューム（log）がこの閾値以上であればエントリー待機

```

#### 注意
自己責任での使用をお願いします。

ライセンス：MIT
