<!-- entry-source-sha256: 2993b26f0600e23f854bc072e7087510e5707cfe35d9b733b6ba5739ac78cfc9 -->

# LumiBot AI Trading

[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · **Français** · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

**Créez des agents de trading IA. Testez-les sur l'historique. Tradez avec la même stratégie.**

LumiBot est un framework Python gratuit et open source pour créer des robots de trading. Écrivez vos règles en Python, confiez les décisions à des agents IA ou combinez les deux. Exécutez des backtests, examinez décisions et transactions, puis connectez un courtier pour le trading simulé ou réel.

Cette page résume le README anglais et la page d'accueil de la documentation. Le code complet figure dans le [README anglais](README.md) et la [documentation](https://lumibot.lumiwealth.com/).

## Pourquoi LumiBot

- **Trading IA natif :** les agents recherchent, raisonnent et passent des ordres à partir de vos instructions.
- **Backtester avant de trader :** évaluez les décisions IA et les règles Python sur des données historiques, avec graphiques, transactions et journaux.
- **La même stratégie en backtest et chez le courtier :** conservez la classe de stratégie ; changez la source de données et la configuration d'exécution.
- **Plusieurs classes d'actifs :** actions, options, contrats à terme, devises, crypto et marchés prédictifs selon les [courtiers pris en charge](https://lumibot.lumiwealth.com/brokers.all.html).
- **La décision atteint un courtier.** Consultez les ordres, les exécutions et les journaux de décision. [Fonctionnement de l’exécution](https://lumibot.lumiwealth.com/execution_gap.html).

## Construisez plus vite avec BotSpot MCP

Connectez gratuitement votre assistant pour accéder aux compétences de création de stratégies LumiBot, aux données de recherche et aux outils de backtest et de déploiement. [Découvrez ce que reçoit votre assistant](#connectez-votre-assistant-ia).

## Trois façons de créer une stratégie

1. **[Stratégies IA natives](README.md#run-your-first-ai-backtest) :** modifiez les prompts de l'agent de trading.
2. **[Stratégies Python](README.md#backtest-a-strategy) :** écrivez les règles en Python.
3. **[Stratégies hybrides](README.md#build-a-hybrid-strategy) :** combinez règles Python et décisions IA.

## Commencer

Python 3.10 ou ultérieur est requis. Lancez un exemple de backtest avec des données quotidiennes gratuites :

```bash
pip install lumibot
lumibot demo
```

Les exemples IA nécessitent les identifiants du modèle ; les appels peuvent être facturés. Le trading simulé ou réel nécessite un compte et les identifiants du courtier. Voir l'[exemple IA](README.md#run-your-first-ai-backtest), la [configuration CLI](https://lumibot.lumiwealth.com/cli.html) et les [modes d'exécution](https://lumibot.lumiwealth.com/strategy_run_modes.html). Un backtest ne garantit pas les résultats futurs.

## Connectez votre assistant IA

Utilisez Claude, ChatGPT, Codex, Cursor, VS Code ou un autre assistant compatible MCP. L'assistant de programmation vous aide à écrire et tester le code ; les agents dans la stratégie prennent ses décisions à l'exécution.

BotSpot MCP propose des compétences de création, des données de recherche gratuites, des backtests avec graphiques, l'hébergement planifié, une communauté, une marketplace et la connexion de comptes de courtage. La connexion est gratuite ; l'utilisation hébergée dépend du forfait.

**[Connectez votre assistant IA](https://botspot.trade/agents)**

Pour l'aide au code en local, installez les [compétences gratuites LumiBot](https://github.com/Lumiwealth/lumibot-trading-skills). Pour les outils de recherche dans une stratégie, voir le [guide BotSpot MCP](https://lumibot.lumiwealth.com/botspot_mcp.html).

## Explorer le framework

[Exemples Python](https://lumibot.lumiwealth.com/examples.html) · [Exemples IA](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [Courtiers](https://lumibot.lumiwealth.com/brokers.all.html)

Le trading comporte un risque de perte. Une simulation historique n'est pas une garantie de rendement.
