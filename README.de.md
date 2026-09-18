[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · **Deutsch** · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI Trading

**KI-Agenten, die die Order tatsächlich aufgeben.** Zwölf Broker-Anbindungen, echte Backtests sowie Aktien, Optionen, Futures, Devisen, Krypto und Prognosemärkte. Die meisten KI-Trading-Projekte enden bei einer Empfehlung. LumiBot schickt die Order ab.

## Sechzig Sekunden

```bash
pip install lumibot
lumibot demo
```

Das führt einen echten Backtest mit kostenlosen Tagesdaten aus und schreibt einen Ergebnisbericht. Ohne API-Schlüssel, ohne Brokerkonto, ohne Konfiguration.

Dann mach ihn zu deinem:

```bash
lumibot init my-bot --template ai
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init` schreibt genau das Python, das du sonst von Hand geschrieben hättest. Die Kommandozeile verbirgt nichts, und du behältst eine bearbeitbare Datei.

**Lieber klassische Strategien?** Nimm `--template python`. Schreib eigene Regeln, Indikatoren und Orderlogik in eine normale `Strategy`-Unterklasse, ohne KI-Modell und ohne Modell-API-Schlüssel.

## Warum LumiBot

- **Python-Regeln, KI-Agenten oder beides.** Ein vertrauter `Strategy`-Lebenszyklus.
- **Backtesten, bevor ein Broker angebunden wird.** Historische Simulationen mit Trades und Ergebnissen.
- **Dieselbe Strategie bei mehreren Brokern.** Strategielogik bleibt von der Brokerkonfiguration getrennt.
- **Von lauffähigen Beispielen starten.** Aktien, Makro, Optionen oder Buy-and-Hold.

## Vergleich

| Projekt | Gibt echte Orders auf | KI-Agenten | Anlageklassen |
|---|---|---|---|
| **Lumibot** | **Ja** (12) | **Ja** | **Ja** |
| TradingAgents | Nein | Ja | Nur Forschung |
| ai-hedge-fund | Nein | Ja | Nur Forschung |
| freqtrade | Ja | Nein | Nur Krypto |
| Hummingbot | Ja | Ja | Nur Krypto |
| OpenBB | Nein | Nein | Nur Forschung |

## Unterstützte Broker

Alpaca, Interactive Brokers (auch REST), Tradier, Schwab, Tradovate, TopstepX-Futures (über ProjectX), Bitunix, Handel und Backtests für Polymarket-Prognosekontrakte sowie ausgewählte CCXT-Krypto-Pfade. LumiBot beansprucht keine pauschale Unterstützung aller CCXT-Börsen.

## Vollständige Dokumentation

Diese Seite fasst das Wichtigste aus der englischen README zusammen. Für den vollständigen Inhalt einschließlich der KI-Team-Beispiele, Datenquellen, Deployment und Beitragsleitfaden:

- Englische README: [README.md](README.md)
- Dokumentation: https://lumibot.lumiwealth.com/
- Gehostete Version: https://botspot.trade

## Haftungsausschluss

Diese Software dient ausschließlich Bildungszwecken. Handel ist mit Risiken verbunden und du kannst Geld verlieren. Frühere Ergebnisse sind kein Hinweis auf künftige Ergebnisse. Dieses Projekt bietet keine Anlageberatung.
