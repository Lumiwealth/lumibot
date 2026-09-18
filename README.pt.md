[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · **Português** · [Русский](README.ru.md)

# LumiBot AI Trading

**Agentes de IA que realmente enviam a ordem.** Doze integrações com corretoras, backtests reais e ações, opções, futuros, câmbio, cripto e mercados de previsão. A maioria dos projetos de trading com IA para numa recomendação. O LumiBot envia a ordem.

## Sessenta segundos

```bash
pip install lumibot
lumibot demo
```

Isso executa um backtest real com dados diários gratuitos e gera um relatório de resultados. Sem chave de API, sem conta em corretora e sem configuração.

Depois torne-o seu:

```bash
lumibot init my-bot --template ai
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

O `lumibot init` escreve o mesmo Python que você escreveria à mão. A linha de comando não esconde nada e você fica com um arquivo editável.

**Prefere estratégias tradicionais?** Use `--template python`. Escreva suas próprias regras, indicadores e lógica de ordens numa subclasse `Strategy` comum, sem modelo de IA e sem chave de API de modelo.

## Por que LumiBot

- **Regras em Python, agentes de IA, ou ambos.** Um único ciclo de vida `Strategy` conhecido.
- **Faça backtest antes de conectar uma corretora.** Simulações históricas com operações e resultados.
- **Reutilize sua estratégia em várias corretoras.** A lógica fica separada da configuração da corretora.
- **Comece por exemplos que funcionam.** Ações, macro, opções ou comprar e manter.

## Como o LumiBot se compara

| Projeto | Envia ordens reais | Agentes de IA | Classes de ativos |
|---|---|---|---|
| **Lumibot** | **Sim** (12) | **Sim** | **Sim** |
| TradingAgents | Não | Sim | Apenas pesquisa |
| ai-hedge-fund | Não | Sim | Apenas pesquisa |
| freqtrade | Sim | Não | Somente cripto |
| Hummingbot | Sim | Sim | Somente cripto |
| OpenBB | Não | Não | Apenas pesquisa |

## Corretoras suportadas

Alpaca, Interactive Brokers (incluindo REST), Tradier, Schwab, Tradovate, futuros TopstepX (via ProjectX), Bitunix, negociação e backtest de contratos de previsão na Polymarket e rotas selecionadas da CCXT para cripto. O LumiBot não afirma suportar todas as exchanges da CCXT.

## Documentação completa

Esta página resume o essencial do README em inglês. Para o conteúdo completo, incluindo os exemplos de equipes de IA, fontes de dados, implantação e guia de contribuição:

- README em inglês: [README.md](README.md)
- Documentação: https://lumibot.lumiwealth.com/
- Versão hospedada: https://botspot.trade

## Aviso legal

Este software é apenas para fins educacionais. Operar envolve risco e você pode perder dinheiro. Resultados passados não indicam resultados futuros. Este projeto não oferece consultoria de investimentos.
