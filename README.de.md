<!-- entry-source-sha256: 8ecf9bafe56a350f0394f277d4c890a90762d87262cbda98cad00a25b44b4460 -->

# LumiBot AI Trading

[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · **Deutsch** · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

**Baue KI-Trading-Agenten. Teste sie mit historischen Daten. Handle mit derselben Strategie.**

LumiBot ist ein kostenloses Open-Source-Python-Framework für Trading-Bots. Schreibe Regeln in Python, lass KI-Agenten entscheiden oder kombiniere beides. Führe historische Backtests aus, prüfe Entscheidungen und Trades und verbinde einen Broker für simulierten oder echten Handel.

Diese Seite fasst die englische README und die Startseite der Dokumentation zusammen. Vollständige Beispiele stehen in der [englischen README](README.md) und der [Dokumentation](https://lumibot.lumiwealth.com/).

## Warum LumiBot

- **Natives KI-Trading:** Agenten recherchieren, bewerten und erteilen Orders anhand deiner Prompts.
- **Vor dem Handel backtesten:** Prüfe KI-Entscheidungen und Python-Regeln mit historischen Daten, Charts, Trades und Logs.
- **Dieselbe Strategie für Backtests und Handel:** Behalte die Strategieklasse; ändere Datenquelle und Broker-Startkonfiguration.
- **Mehrere Anlageklassen:** Aktien, Optionen, Futures, Devisen, Krypto und Prognosemärkte je nach [unterstütztem Broker](https://lumibot.lumiwealth.com/brokers.all.html).
- **Die Entscheidung erreicht einen Broker.** Prüfe Orders, Ausführungen und Entscheidungsprotokolle. [So funktioniert die Ausführung](https://lumibot.lumiwealth.com/execution_gap.html).

## Schneller entwickeln mit BotSpot MCP

Verbinde deinen Assistenten kostenlos und erhalte LumiBot-Strategie-Skills, Forschungsdaten sowie Werkzeuge für Backtests und Bereitstellung. [Sieh dir die Funktionen an](#verbinde-deinen-ki-assistenten).

## Drei Wege zur Strategie

1. **[KI-native Strategien](README.md#run-your-first-ai-backtest):** Bearbeite die Prompts des Trading-Agenten.
2. **[Python-Strategien](README.md#backtest-a-strategy):** Schreibe deine Regeln in Python.
3. **[Hybride Strategien](README.md#build-a-hybrid-strategy):** Kombiniere Python-Regeln und KI-Entscheidungen.

## Einstieg

Python 3.10 oder neuer ist erforderlich. Starte einen Beispiel-Backtest mit kostenlosen Tagesdaten:

```bash
pip install lumibot
lumibot demo
```

KI-Beispiele benötigen Modell-Zugangsdaten; Modellaufrufe können Kosten verursachen. Simulierter oder echter Handel benötigt ein Brokerkonto und dessen Zugangsdaten. Siehe [KI-Beispiel](README.md#run-your-first-ai-backtest), [CLI-Einrichtung](https://lumibot.lumiwealth.com/cli.html) und [Ausführungsmodi](https://lumibot.lumiwealth.com/strategy_run_modes.html). Backtests garantieren keine zukünftigen Ergebnisse.

## Verbinde deinen KI-Assistenten

Nutze Claude, ChatGPT, Codex, Cursor, VS Code oder einen anderen MCP-kompatiblen Assistenten. Der Programmierassistent hilft beim Schreiben und Testen des Codes; die Agenten in der Strategie treffen ihre Entscheidungen während der Ausführung.

BotSpot MCP bietet Strategie-Skills, kostenlose Forschungsdaten, Backtests mit Charts, geplantes Cloud-Hosting, Community, Marktplatz und die Verknüpfung von Brokerkonten. Die Verbindung ist kostenlos; gehostete Nutzung hängt vom Tarif ab.

**[Verbinde deinen KI-Assistenten](https://botspot.trade/agents)**

Für lokale Programmierhilfe gibt es die [kostenlosen LumiBot-Trading-Skills](https://github.com/Lumiwealth/lumibot-trading-skills). Forschungswerkzeuge innerhalb einer Strategie erklärt der [BotSpot-MCP-Leitfaden](https://lumibot.lumiwealth.com/botspot_mcp.html).


Mit `lumibot connect botspot` erhalten Strategieagenten Forschungswerkzeuge für Daten, Dokumente und erlaubte Marketplace- und Community-Inhalte. LumiBot funktioniert auch eigenständig.

## Framework erkunden

[Python-Beispiele](https://lumibot.lumiwealth.com/examples.html) · [KI-Beispiele](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [Broker](https://lumibot.lumiwealth.com/brokers.all.html)

Beim Trading sind Verluste möglich. Eine historische Simulation garantiert keine Rendite.
