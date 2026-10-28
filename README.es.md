<!-- entry-source-sha256: 7e4b9f55cd1699da8c65e25a9decaff4608e9ba1e7bf7bc06e5ffb5bcca95206 -->

# LumiBot AI Trading

[English](README.md) · [中文](README.zh-CN.md) · **Español** · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

**Crea agentes de trading con IA. Haz backtests. Opera con la misma estrategia.**

LumiBot es un framework de Python gratuito y de código abierto para crear bots de trading. Escribe reglas en Python, deja que los agentes de IA tomen decisiones o combina ambos. Ejecuta backtests históricos, revisa decisiones y operaciones y conecta un bróker para operar en simulado o en real.

Este es un resumen localizado del README en inglés y de la página principal de la documentación. El código completo está en el [README en inglés](README.md) y la [documentación](https://lumibot.lumiwealth.com/).

## Por qué LumiBot

- **Trading nativo con IA:** los agentes investigan, razonan y envían órdenes a partir de tus instrucciones.
- **Backtest antes de operar:** evalúa decisiones de IA y reglas de Python con datos históricos, gráficos, operaciones y registros.
- **La misma estrategia para backtesting y trading:** conserva la clase de estrategia; cambia la fuente de datos y la configuración del bróker.
- **Varias clases de activos:** acciones, opciones, futuros, divisas, cripto y mercados de predicción, según los [brókers compatibles](https://lumibot.lumiwealth.com/brokers.all.html).
- **La decisión llega a un bróker.** Revisa las órdenes, las ejecuciones y los registros de decisiones. [Cómo funciona la ejecución](https://lumibot.lumiwealth.com/execution_gap.html).

## Construye más rápido con BotSpot MCP

Conecta gratis tu asistente para acceder a habilidades de desarrollo de estrategias LumiBot, datos de investigación y herramientas de backtesting y despliegue. [Consulta qué obtiene tu asistente](#conecta-tu-asistente-de-ia).

## Dale superpoderes a tus agentes, gratis

Conecta BotSpot MCP a tu estrategia LumiBot: datos económicos, noticias, documentos de la SEC, estrategias del Marketplace y conocimiento de la comunidad.

![Dale superpoderes a tus agentes, gratis](docs/assets/readme/botspot_mcp_superpowers.png)

¿Tu asistente ya está conectado a BotSpot? Dile: «Conecta este proyecto LumiBot a BotSpot MCP». Puede crear la clave API y guardar la configuración por ti.

**[Conecta tus agentes](https://lumibot.lumiwealth.com/botspot_mcp.html)**

## Tres formas de crear una estrategia

1. **[Estrategias nativas de IA](README.md#run-your-first-ai-backtest):** edita los prompts del agente de trading.
2. **[Estrategias de Python](README.md#backtest-a-strategy):** escribe las reglas en Python.
3. **[Estrategias híbridas](README.md#build-a-hybrid-strategy):** combina reglas de Python y decisiones de IA.

## Primeros pasos

Necesitas Python 3.10 o posterior. Ejecuta un backtest de ejemplo con datos diarios gratuitos:

```bash
pip install lumibot
lumibot demo
```

Los ejemplos de IA requieren credenciales del modelo; las llamadas pueden tener coste. Operar en simulado o en real requiere una cuenta y credenciales del bróker. Consulta el [ejemplo de IA](README.md#run-your-first-ai-backtest), la [configuración de la CLI](https://lumibot.lumiwealth.com/cli.html) y los [modos de ejecución](https://lumibot.lumiwealth.com/strategy_run_modes.html). Un backtest no garantiza resultados futuros.

## Conecta tu asistente de IA

Usa Claude, ChatGPT, Codex, Cursor, VS Code u otro asistente compatible con MCP. El asistente de programación ayuda a crear y probar el código; los agentes dentro de la estrategia toman las decisiones durante su ejecución.

BotSpot MCP ofrece habilidades de desarrollo, datos de investigación gratuitos, backtests con gráficos, alojamiento programado, comunidad, marketplace y conexión de cuentas de bróker. Conectarse es gratis; el uso alojado depende del plan.

**[Conecta tu asistente de IA](https://botspot.trade/agents)**

Para ayuda local de programación, instala las [habilidades gratuitas de LumiBot](https://github.com/Lumiwealth/lumibot-trading-skills). Para herramientas de investigación dentro de la estrategia, consulta la [guía de BotSpot MCP](https://lumibot.lumiwealth.com/botspot_mcp.html).

## Explora el framework

[Ejemplos de Python](https://lumibot.lumiwealth.com/examples.html) · [Ejemplos de IA](https://lumibot.lumiwealth.com/agents_examples.html) · [Strategy API](https://lumibot.lumiwealth.com/strategy_api_overview.html) · [Brókers](https://lumibot.lumiwealth.com/brokers.all.html)

El trading implica riesgo y puede causar pérdidas. Una simulación histórica no garantiza rentabilidad.
