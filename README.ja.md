[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · **日本語** · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI Trading

**実際に注文を出す AI エージェント。** 12 のブローカー連携、本物のバックテスト、そして株式・オプション・先物・為替・暗号資産・予測市場に対応します。ほとんどの AI トレーディングプロジェクトは提案で終わります。LumiBot は注文を送信します。

## 60 秒で試す

```bash
pip install lumibot
lumibot demo
```

無料の日足データで実際のバックテストを実行し、成績レポートを出力します。API キーもブローカー口座も設定も不要です。

次に、自分のものにします:

```bash
lumibot init my-bot --template ai
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init` が書き出すのは、あなたが手で書いたはずの Python そのものです。コマンドラインは何も隠しません。編集可能なファイルが手元に残ります。

**従来型の戦略がよいですか。** `--template python` を使ってください。AI モデルもモデル API キーも使わず、通常の `Strategy` サブクラスに独自のルール、指標、発注ロジックを書けます。

## LumiBot を選ぶ理由

- **Python のルール、AI エージェント、またはその両方。** 使い慣れた 1 つの `Strategy` ライフサイクル。
- **ブローカー接続の前にバックテスト。** ヒストリカル検証で取引と結果を確認できます。
- **同じ戦略を複数のブローカーで再利用。** 戦略ロジックとブローカー設定を分離します。
- **動く実例から始める。** 株式、マクロ、オプション、またはバイ・アンド・ホールド。

## 他プロジェクトとの比較

| プロジェクト | 実際に発注できるか | AI エージェント | 対象資産 |
|---|---|---|---|
| **Lumibot** | **対応** (12) | **対応** | **対応** |
| TradingAgents | 非対応 | 対応 | リサーチのみ |
| ai-hedge-fund | 非対応 | 対応 | リサーチのみ |
| freqtrade | 対応 | 非対応 | 暗号資産のみ |
| Hummingbot | 対応 | 対応 | 暗号資産のみ |
| OpenBB | 非対応 | 非対応 | リサーチのみ |

## 対応ブローカー

Alpaca、Interactive Brokers（REST を含む）、Tradier、Schwab、Tradovate、TopstepX 先物（ProjectX 経由）、Bitunix、Polymarket 予測コントラクトの取引とバックテスト、および一部の CCXT 暗号資産経路。LumiBot はすべての CCXT 取引所への対応を主張するものではありません。

## 完全なドキュメント

このページは英語版 README の要点をまとめたものです。AI チームの実例、データソース、デプロイ、コントリビューションガイドを含む全内容は次を参照してください。

- 英語版 README: [README.md](README.md)
- ドキュメント: https://lumibot.lumiwealth.com/
- ホスティング版: https://botspot.trade

## 免責事項

本ソフトウェアは教育目的でのみ提供されます。取引にはリスクが伴い、損失が生じる可能性があります。過去の実績は将来の結果を示すものではありません。本プロジェクトは投資助言を提供しません。
