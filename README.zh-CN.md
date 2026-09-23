[English](README.md) · **中文** · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI 交易框架

**真正会下单的 AI 交易智能体。** 支持十二个券商与交易所接入、真实回测，覆盖股票、期权、期货、外汇、加密货币与预测市场。大多数 AI 交易项目只给出建议，LumiBot 会把订单发出去。

## 六十秒上手

```bash
pip install lumibot
lumibot demo
```

这会用免费的日线数据跑一次真实回测，并生成业绩报告。不需要 API 密钥，不需要券商账户，不需要任何配置。

然后把它变成你自己的策略：

```bash
lumibot init my-bot --template ai     # 生成一个普通的、可直接编辑的 Strategy 子类
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init` 写出来的就是你本来会手写的 Python 代码。命令行没有隐藏任何东西，你拿到的是一个可以随意修改的文件。

**偏好传统量化策略？** 改用 `--template python`。在普通的 `Strategy` 子类里编写自己的规则、指标和下单逻辑，不需要 AI 模型，也不需要模型 API 密钥。

## 为什么选择 LumiBot

- **Python 规则、AI 智能体，或者两者结合。** 共用同一套熟悉的 `Strategy` 生命周期。
- **先回测，再接券商。** 运行历史模拟，查看每一笔交易和结果。
- **同一份策略可复用于多个券商。** 策略逻辑与券商配置相互分离。
- **从可运行的示例开始。** 股票、宏观、期权，或传统的买入持有策略。

## 与其他项目的对比

AI 交易项目已经证明了市场需要智能体交易流程。LumiBot 的优势在于，这些流程运行在一个真实的 Python 交易框架内：你可以回测智能体的决策、检查产物、加上 Python 风控护栏、模拟盘交易，并连接券商，而无需重写策略。

这一点很重要，因为一个 AI 交易演示并不等于一套交易系统。没有回测、没有面向券商的策略代码，你基本上只是在相信提示词。

### 与 AI 交易智能体项目对比

| 项目 | 主要定位 | AI 智能体 / 团队 | 回测智能体决策 | 模拟盘 / 实盘券商路径 | 确定性 Python 策略 |
|---|---|---|---|---|---|
| **Lumibot + BotSpot** | Python 策略、灵活的 AI 交易团队、混合风控、回测、券商、托管部署 | **灵活的团队、辩论机制、专家小组与确定性闸门** | **可重放的决策、订单、追踪、产物、图表与日志** | **支持：Alpaca、盈透证券、Tradier、嘉信理财、Tradovate、ProjectX、Bitunix、Polymarket、部分 CCXT** | **支持** |
| TradingAgents | 多智能体 LLM 交易研究框架 | 支持，采用特定的研究/辩论结构 | 偏研究与演示 | 非其重点 | 有限 |
| ai-hedge-fund | 教学用途的 AI 对冲基金，带投资人风格角色 | 支持，投资人风格角色 | 偏演示与回测 | 非其重点 | 有限 |
| OpenBB | 面向分析师、量化与 AI 智能体的金融数据平台 | 为智能体提供工具 | 不是策略回测器 | 无券商执行框架 | 否 |
| Qlib | 面向 AI 的量化研究平台 | 研究 / 机器学习 | 量化研究回测 | 实盘支持有限 | 研究流水线 |

### 与回测库对比

| 特性 | Lumibot | Backtrader | Freqtrade | Zipline | Jesse | NautilusTrader | Hummingbot |
|---|---|---|---|---|---|---|---|
| **同一份代码：回测 + 实盘** | 是 | 是 | 是（加密） | 否 | 是（付费） | 是 | 是（加密） |
| **股票** | 是 | 是 | 否 | 是 | 否 | 是 | 否 |
| **期权** | **是** | 否 | 否 | 否 | 否 | 有限 | 否 |
| **加密货币** | 是 | 有限 | 是 | 否 | 是 | 是 | 是 |
| **预测市场** | Polymarket 交易与回测 | 否 | 否 | 否 | 否 | 否 | 有限 / 否 |
| **期货** | 是 | 有限 | 仅加密 | 部分 | 仅加密 | 是 | 加密永续 |
| **AI 智能体运行时** | 内置 | 否 | FreqAI（机器学习） | 否 | 机器学习流水线 | 否 | 脚本 / 控制器 |
| **托管部署路径** | BotSpot | 否 | 否 | 否 | 付费云 | 否 | Hummingbot 基金会 / 企业生态 |

## 支持的券商与交易所

Alpaca、盈透证券（含 REST）、Tradier、嘉信理财、Tradovate、TopstepX 期货（通过 ProjectX）、Bitunix、Polymarket 预测合约交易与回测，以及部分 CCXT 加密路径。

LumiBot 并不声称支持全部 CCXT 交易所。具体覆盖范围请查阅文档。

## 完整文档

本页面是英文 README 的重点摘要。完整内容，包括 AI 交易团队示例、数据源、部署方式与贡献指南，请参阅：

- 英文 README：[README.md](README.md)
- 文档站：https://lumibot.lumiwealth.com/
- 托管版本：https://botspot.trade

## 免责声明

本软件仅供教育用途。交易有风险，可能造成本金损失。过往表现不代表未来结果。本项目不提供投资建议。
