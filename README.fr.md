[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · **Français** · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI Trading

**Des agents IA qui passent vraiment l'ordre.** Douze intégrations de courtiers, des backtests réels, et des actions, options, futures, devises, cryptos et marchés de prédiction. La plupart des projets de trading par IA s'arrêtent à une recommandation. LumiBot envoie l'ordre.

## Soixante secondes

```bash
pip install lumibot
lumibot demo
```

Cela lance un vrai backtest sur des données journalières gratuites et génère un rapport de performance. Sans clé API, sans compte de courtier et sans configuration.

Ensuite, appropriez-vous-le :

```bash
lumibot init my-bot --template ai
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init` écrit le même Python que vous auriez écrit à la main. La ligne de commande ne cache rien et vous repartez avec un fichier modifiable.

**Vous préférez les stratégies traditionnelles ?** Utilisez `--template python`. Écrivez vos propres règles, indicateurs et logique d'ordres dans une sous-classe `Strategy` ordinaire, sans modèle d'IA ni clé API de modèle.

## Pourquoi LumiBot

- **Règles Python, agents IA, ou les deux.** Un seul cycle de vie `Strategy` familier.
- **Backtestez avant de connecter un courtier.** Simulations historiques, avec les transactions et les résultats.
- **Réutilisez votre stratégie chez plusieurs courtiers.** La logique reste séparée de la configuration du courtier.
- **Partez d'exemples qui fonctionnent.** Actions, macro, options, ou achat-conservation.

## Comparaison

| Projet | Passe de vrais ordres | Agents IA | Classes d'actifs |
|---|---|---|---|
| **Lumibot** | **Oui** (12) | **Oui** | **Oui** |
| TradingAgents | Non | Oui | Recherche uniquement |
| ai-hedge-fund | Non | Oui | Recherche uniquement |
| freqtrade | Oui | Non | Cryptos uniquement |
| Hummingbot | Oui | Oui | Cryptos uniquement |
| OpenBB | Non | Non | Recherche uniquement |

## Courtiers pris en charge

Alpaca, Interactive Brokers (y compris REST), Tradier, Schwab, Tradovate, futures TopstepX (via ProjectX), Bitunix, trading et backtest de contrats de prédiction Polymarket, et certaines routes CCXT pour les cryptos. LumiBot ne prétend pas prendre en charge toutes les places CCXT.

## Documentation complète

Cette page résume l'essentiel du README anglais. Pour le contenu complet, y compris les exemples d'équipes d'agents, les sources de données, le déploiement et le guide de contribution :

- README anglais: [README.md](README.md)
- Documentation: https://lumibot.lumiwealth.com/
- Version hébergée: https://botspot.trade

## Avertissement

Ce logiciel est fourni à des fins éducatives uniquement. Le trading comporte des risques et vous pouvez perdre de l'argent. Les performances passées ne préjugent pas des performances futures. Ce projet ne fournit aucun conseil en investissement.
