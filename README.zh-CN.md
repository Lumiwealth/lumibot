<!-- entry-source-sha256: 2993b26f0600e23f854bc072e7087510e5707cfe35d9b733b6ba5739ac78cfc9 -->

# LumiBot AI 交易框架

[English](README.md) · **中文** · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

**构建 AI 交易智能体，先回测，再用同一套策略连接券商。**

LumiBot 是一个免费、开源的 Python 交易机器人框架。你可以编写 Python 交易规则，让 AI 智能体做决策，或结合两者。框架支持历史回测、决策与交易记录，以及模拟盘或实盘券商连接。

这是英文 README 和文档首页的本地化摘要。完整代码与详细说明请参阅 [English README](README.md) 和[文档](https://lumibot.lumiwealth.com/)。

## 为什么选择 LumiBot

- **原生 AI 交易：** 通过提示词让智能体研究、推理和下单。
- **先回测，再交易：** 用历史数据评估 AI 决策与 Python 规则，查看图表、交易和日志。
- **回测和交易使用同一套策略：** 保留策略类，切换数据源和券商运行配置。
- **多资产类别：** 支持股票、期权、期货、外汇、加密货币和预测市场，具体取决于[券商支持范围](https://lumibot.lumiwealth.com/brokers.all.html)。
- **决策会传达到券商。** 查看订单、成交和决策记录。[执行机制说明](https://lumibot.lumiwealth.com/execution_gap.html).

## 借助 BotSpot MCP 更快构建

免费连接你的 AI 助手，获取 LumiBot 策略构建技能、研究数据，以及回测和部署工具。[查看助手可以获得什么](#连接你的-ai-助手)。

## 三种策略构建方式

1. **[AI 原生策略](README.md#run-your-first-ai-backtest)：** 修改交易智能体的提示词。
2. **[Python 策略](README.md#backtest-a-strategy)：** 用 Python 编写交易规则。
3. **[混合策略](README.md#build-a-hybrid-strategy)：** 结合 Python 规则与 AI 决策。

## 开始使用

需要 Python 3.10 或更高版本。运行使用免费日线数据的示例回测：

```bash
pip install lumibot
lumibot demo
```

AI 示例需要模型凭据；模型调用可能收费。模拟盘或实盘交易需要相应券商的账户和凭据。查看 [AI 示例](README.md#run-your-first-ai-backtest)、[CLI 配置](https://lumibot.lumiwealth.com/cli.html)和[运行模式](https://lumibot.lumiwealth.com/strategy_run_modes.html)。回测结果不保证未来表现。

## 连接你的 AI 助手

支持 Claude、ChatGPT、Codex、Cursor、VS Code 或其他兼容 MCP 的助手。编程助手帮助你构建和测试代码；策略内的智能体负责运行时的交易决策。

BotSpot MCP 提供策略构建技能、免费研究数据、带图表的回测、定时云托管、社区、策略市场和券商账户连接。连接免费；托管使用额度取决于套餐。

**[连接你的 AI 助手](https://botspot.trade/agents)**

本地编程辅助也可使用免费的 [LumiBot 交易技能](https://github.com/Lumiwealth/lumibot-trading-skills)。策略内的研究工具见 [BotSpot MCP 指南](https://lumibot.lumiwealth.com/botspot_mcp.html)。

## 继续探索

[策略示例](https://lumibot.lumiwealth.com/examples.html) · [AI 示例](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [券商](https://lumibot.lumiwealth.com/brokers.all.html)

交易有风险，可能亏损。历史模拟不是收益保证。
