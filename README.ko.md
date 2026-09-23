[English](README.md) · [中文](README.zh-CN.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · **한국어** · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI Trading

**실제로 주문을 내는 AI 에이전트.** 12개 브로커 연동, 진짜 백테스트, 그리고 주식·옵션·선물·외환·암호화폐·예측 시장을 지원합니다. 대부분의 AI 트레이딩 프로젝트는 추천에서 멈춥니다. LumiBot은 주문을 전송합니다.

## 60초 만에 시작

```bash
pip install lumibot
lumibot demo
```

무료 일봉 데이터로 실제 백테스트를 실행하고 성과 리포트를 생성합니다. API 키도, 증권 계좌도, 설정도 필요 없습니다.

그다음 내 것으로 만듭니다:

```bash
lumibot init my-bot --template ai
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init`이 작성하는 코드는 여러분이 직접 손으로 썼을 Python 그대로입니다. 명령줄이 숨기는 것은 없으며, 편집 가능한 파일이 남습니다.

**전통적인 전략을 선호하시나요?** `--template python`을 사용하세요. AI 모델도 모델 API 키도 없이, 일반 `Strategy` 서브클래스에 직접 규칙과 지표, 주문 로직을 작성할 수 있습니다.

## 왜 LumiBot인가

- **Python 규칙, AI 에이전트, 또는 둘 다.** 익숙한 하나의 `Strategy` 생명주기.
- **브로커 연결 전에 백테스트.** 과거 데이터로 시뮬레이션하고 거래와 결과를 확인합니다.
- **같은 전략을 여러 브로커에서 재사용.** 전략 로직과 브로커 설정을 분리합니다.
- **동작하는 예제에서 시작.** 주식, 매크로, 옵션, 또는 매수 후 보유.

## 비교

| 프로젝트 | 실제 주문 실행 | AI 에이전트 | 자산군 |
|---|---|---|---|
| **Lumibot** | **예** (12) | **예** | **예** |
| TradingAgents | 아니오 | 예 | 연구 전용 |
| ai-hedge-fund | 아니오 | 예 | 연구 전용 |
| freqtrade | 예 | 아니오 | 암호화폐 전용 |
| Hummingbot | 예 | 예 | 암호화폐 전용 |
| OpenBB | 아니오 | 아니오 | 연구 전용 |

## 지원 브로커

Alpaca, Interactive Brokers(REST 포함), Tradier, Schwab, Tradovate, TopstepX 선물(ProjectX 경유), Bitunix, Polymarket 예측 계약 거래 및 백테스트, 그리고 일부 CCXT 암호화폐 경로를 지원합니다. LumiBot은 모든 CCXT 거래소 지원을 주장하지 않습니다.

## 전체 문서

이 페이지는 영문 README의 핵심을 요약한 것입니다. AI 팀 예제, 데이터 소스, 배포, 기여 가이드를 포함한 전체 내용은 다음을 참고하세요.

- 영문 README: [README.md](README.md)
- 문서: https://lumibot.lumiwealth.com/
- 호스팅 버전: https://botspot.trade

## 면책 조항

이 소프트웨어는 교육 목적으로만 제공됩니다. 거래에는 위험이 따르며 손실이 발생할 수 있습니다. 과거 성과가 미래 결과를 보장하지 않습니다. 이 프로젝트는 투자 자문을 제공하지 않습니다.
