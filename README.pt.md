<!-- entry-source-sha256: 8ecf9bafe56a350f0394f277d4c890a90762d87262cbda98cad00a25b44b4460 -->

# LumiBot AI Trading

[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · **Português** · [Русский](README.ru.md)

**Crie agentes de trading com IA. Faça backtests. Opere com a mesma estratégia.**

O LumiBot é um framework Python gratuito e de código aberto para criar bots de trading. Escreva regras em Python, deixe os agentes de IA tomarem decisões ou combine os dois. Rode backtests históricos, examine decisões e operações e conecte uma corretora para operar em simulação ou com dinheiro real.

Esta página resume o README em inglês e a página inicial da documentação. O código completo está no [README em inglês](README.md) e na [documentação](https://lumibot.lumiwealth.com/).

## Por que LumiBot

- **Trading nativo com IA:** agentes pesquisam, raciocinam e enviam ordens com base nos seus prompts.
- **Backtest antes de operar:** avalie decisões da IA e regras Python com dados históricos, gráficos, operações e registros.
- **A mesma estratégia para backtest e trading:** mantenha a classe da estratégia; troque a fonte de dados e a configuração de execução da corretora.
- **Várias classes de ativos:** ações, opções, futuros, câmbio, cripto e mercados de previsão, conforme as [corretoras compatíveis](https://lumibot.lumiwealth.com/brokers.all.html).
- **A decisão chega a uma corretora.** Consulte ordens, execuções e registros de decisão. [Como a execução funciona](https://lumibot.lumiwealth.com/execution_gap.html).

## Desenvolva mais rápido com BotSpot MCP

Conecte seu assistente gratuitamente para acessar habilidades de criação de estratégias LumiBot, dados de pesquisa e ferramentas de backtest e implantação. [Veja o que seu assistente recebe](#conecte-seu-assistente-de-ia).

## Três maneiras de criar uma estratégia

1. **[Estratégias nativas de IA](README.md#run-your-first-ai-backtest):** edite os prompts do agente de trading.
2. **[Estratégias Python](README.md#backtest-a-strategy):** escreva as regras em Python.
3. **[Estratégias híbridas](README.md#build-a-hybrid-strategy):** combine regras Python e decisões da IA.

## Primeiros passos

É necessário Python 3.10 ou superior. Execute um backtest de exemplo com dados diários gratuitos:

```bash
pip install lumibot
lumibot demo
```

Os exemplos de IA exigem credenciais do modelo; as chamadas podem ter custo. Operar em simulação ou com dinheiro real exige conta e credenciais da corretora. Consulte o [exemplo de IA](README.md#run-your-first-ai-backtest), a [configuração da CLI](https://lumibot.lumiwealth.com/cli.html) e os [modos de execução](https://lumibot.lumiwealth.com/strategy_run_modes.html). Um backtest não garante resultados futuros.

## Conecte seu assistente de IA

Use Claude, ChatGPT, Codex, Cursor, VS Code ou outro assistente compatível com MCP. O assistente de programação ajuda a escrever e testar o código; os agentes dentro da estratégia tomam as decisões durante a execução.

O BotSpot MCP oferece habilidades de criação, dados de pesquisa gratuitos, backtests com gráficos, hospedagem agendada, comunidade, marketplace e conexão de contas de corretora. A conexão é gratuita; o uso hospedado depende do plano.

**[Conecte seu assistente de IA](https://botspot.trade/agents)**

Para ajuda local de programação, instale as [habilidades gratuitas de trading LumiBot](https://github.com/Lumiwealth/lumibot-trading-skills). Para ferramentas de pesquisa dentro da estratégia, consulte o [guia BotSpot MCP](https://lumibot.lumiwealth.com/botspot_mcp.html).


Execute `lumibot connect botspot` para dar aos agentes da estratégia ferramentas de pesquisa de dados, documentos e informações permitidas do marketplace e da comunidade. O LumiBot também funciona de forma independente.

## Explore o framework

[Exemplos Python](https://lumibot.lumiwealth.com/examples.html) · [Exemplos de IA](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [Corretoras](https://lumibot.lumiwealth.com/brokers.all.html)

Operar envolve risco de perda. Uma simulação histórica não garante rentabilidade.
