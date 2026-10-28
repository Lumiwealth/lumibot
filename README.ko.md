<!-- entry-source-sha256: 5b732c279d9628ef9355c3c2c17fbcd268393bf84b752f6f2eac33bbfeb3137f -->

# LumiBot AI Trading

[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · **한국어** · [Português](README.pt.md) · [Русский](README.ru.md)

**AI 트레이딩 에이전트를 만들고 백테스트한 뒤, 같은 전략으로 거래하세요.**

LumiBot은 트레이딩 봇을 만드는 무료 오픈소스 Python 프레임워크입니다. Python 규칙, AI 에이전트의 판단 또는 둘을 결합해 전략을 만들 수 있습니다. 과거 데이터로 테스트하고 판단과 거래를 확인한 뒤 모의 또는 실거래 브로커에 연결합니다.

이 페이지는 영어 README와 문서 홈페이지의 요약입니다. 전체 코드는 [영어 README](README.md)와 [문서](https://lumibot.lumiwealth.com/)에서 확인하세요.

## LumiBot을 사용하는 이유

- **네이티브 AI 트레이딩:** 프롬프트에 따라 에이전트가 조사하고 추론하며 주문합니다.
- **거래 전에 백테스트:** 과거 데이터로 AI 판단과 Python 규칙을 평가하고 차트, 거래, 로그를 확인합니다.
- **백테스트와 거래에 같은 전략 사용:** 전략 클래스를 유지하고 데이터 소스와 브로커 실행 설정을 바꿉니다.
- **여러 자산군 지원:** [지원 브로커](https://lumibot.lumiwealth.com/brokers.all.html)에 따라 주식, 옵션, 선물, 외환, 암호화폐 및 예측 시장을 다룹니다.
- **의사결정이 브로커에게 전달됩니다.** 주문, 체결 및 의사결정 기록을 확인하세요. [실행 방식](https://lumibot.lumiwealth.com/execution_gap.html).

## BotSpot MCP로 더 빠르게 구축

어시스턴트를 무료로 연결하면 LumiBot 전략 작성 스킬, 조사 데이터, 백테스트 및 배포 도구를 사용할 수 있습니다. [제공되는 기능 확인](#ai-어시스턴트-연결).

## 전략을 만드는 세 가지 방법

1. **[AI 네이티브 전략](README.md#run-your-first-ai-backtest):** 트레이딩 에이전트의 프롬프트를 수정합니다.
2. **[Python 전략](README.md#backtest-a-strategy):** Python으로 규칙을 작성합니다.
3. **[하이브리드 전략](README.md#build-a-hybrid-strategy):** Python 규칙과 AI 판단을 결합합니다.

## 시작하기

Python 3.10 이상이 필요합니다. 무료 일봉 데이터를 사용하는 예제 백테스트를 실행하세요.

```bash
pip install lumibot
lumibot demo
```

AI 예제에는 모델 인증 정보가 필요하며 호출 비용이 발생할 수 있습니다. 모의 또는 실거래에는 브로커 계정과 인증 정보가 필요합니다. [AI 예제](README.md#run-your-first-ai-backtest), [CLI 설정](https://lumibot.lumiwealth.com/cli.html), [실행 모드](https://lumibot.lumiwealth.com/strategy_run_modes.html)를 확인하세요. 백테스트는 미래 성과를 보장하지 않습니다.

## AI 어시스턴트 연결

Claude, ChatGPT, Codex, Cursor, VS Code 또는 다른 MCP 호환 어시스턴트를 사용할 수 있습니다. 코딩 어시스턴트는 코드 작성과 테스트를 돕고, 전략 내부 에이전트는 실행 중 거래 판단을 합니다.

BotSpot MCP는 전략 작성 스킬, 무료 조사 데이터, 차트가 포함된 백테스트, 예약 클라우드 실행, 커뮤니티, 마켓플레이스 및 브로커 계정 연결을 제공합니다. 연결은 무료이며 호스팅된 기능의 사용량은 요금제에 따라 다릅니다.

**[AI 어시스턴트 연결](https://botspot.trade/agents)**

로컬 코딩 지원에는 무료 [LumiBot 트레이딩 스킬](https://github.com/Lumiwealth/lumibot-trading-skills)을 사용할 수 있습니다. 전략 내부 조사 도구는 [BotSpot MCP 가이드](https://lumibot.lumiwealth.com/botspot_mcp.html)를 참고하세요.

## 프레임워크 살펴보기

[Python 예제](https://lumibot.lumiwealth.com/examples.html) · [AI 예제](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [브로커](https://lumibot.lumiwealth.com/brokers.all.html)

거래에는 손실 위험이 있습니다. 과거 시뮬레이션은 수익을 보장하지 않습니다.
