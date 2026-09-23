[English](README.md) · [中文](README.zh-CN.md) · **Español** · [Français](README.fr.md) · [Deutsch](README.de.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Português](README.pt.md) · [Русский](README.ru.md)

# LumiBot AI Trading

**Agentes de IA que realmente envían la orden.** Doce integraciones con brókers, backtests reales, y acciones, opciones, futuros, divisas, cripto y mercados de predicción. La mayoría de los proyectos de trading con IA se detienen en una recomendación. LumiBot envía la orden.

## Sesenta segundos

```bash
pip install lumibot
lumibot demo
```

Eso ejecuta un backtest real con datos diarios gratuitos y genera un informe de resultados. Sin clave de API, sin cuenta de bróker y sin configuración.

Después hazlo tuyo:

```bash
lumibot init my-bot --template ai     # escribe una subclase de Strategy normal y editable
lumibot backtest my-bot --days 90
lumibot run my-bot --paper
```

`lumibot init` escribe el mismo Python que habrías escrito a mano. La línea de comandos no oculta nada y te quedas con un archivo que puedes editar.

**¿Prefieres estrategias tradicionales?** Usa `--template python`. Escribe tus propias reglas, indicadores y lógica de órdenes en una subclase de `Strategy` normal, sin modelo de IA y sin clave de API de ningún modelo.

## Por qué LumiBot

- **Reglas en Python, agentes de IA, o ambos.** Un único ciclo de vida `Strategy` conocido.
- **Haz backtest antes de conectar un bróker.** Ejecuta simulaciones históricas y revisa operaciones y resultados.
- **Reutiliza tu estrategia en varios brókers.** La lógica de la estrategia queda separada de la configuración del bróker.
- **Empieza desde ejemplos que funcionan.** Acciones, macro, opciones o una estrategia tradicional de comprar y mantener.

## Cómo se compara LumiBot

Los proyectos de trading con IA han demostrado que la gente quiere flujos de trabajo con agentes. La ventaja de LumiBot es que esos flujos se ejecutan dentro de un framework de trading real en Python: puedes hacer backtest de las decisiones del agente, inspeccionar los artefactos, añadir barreras de riesgo en Python, operar en simulado y conectar brókers sin reescribir la estrategia.

Esto importa porque una demo de trading con IA no es lo mismo que un sistema de trading. Sin backtests y sin código de estrategia consciente del bróker, básicamente estás confiando en los prompts.

### Comparado con proyectos de agentes de trading con IA

| Proyecto | Enfoque principal | Agentes / equipos de IA | Backtest de decisiones del agente | Ruta a bróker simulado o real | Estrategias deterministas en Python |
|---|---|---|---|---|---|
| **Lumibot + BotSpot** | Estrategias en Python, equipos de IA flexibles, barreras híbridas, backtests, brókers, despliegue gestionado | **Equipos flexibles, debates, mesas especializadas y filtros deterministas** | **Decisiones, órdenes, trazas, artefactos, gráficos y registros reproducibles** | **Sí: Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket, CCXT seleccionados** | **Sí** |
| TradingAgents | Framework de investigación multiagente con LLM | Sí, con una estructura concreta de investigación y debate | Orientado a investigación y demo | No es su objetivo principal | Limitado |
| ai-hedge-fund | Fondo de cobertura con IA, educativo, con personajes de inversores | Sí, con personajes de inversores | Orientado a demo y backtest | No es su objetivo principal | Limitado |
| OpenBB | Plataforma de datos financieros para analistas, quants y agentes | Herramientas para agentes | No es un motor de backtest de estrategias | Sin framework de ejecución en bróker | No |
| Qlib | Plataforma de investigación cuantitativa orientada a IA | Investigación y aprendizaje automático | Backtests de investigación cuantitativa | Enfoque limitado en tiempo real | Pipelines de investigación |

### Comparado con librerías de backtesting

| Característica | Lumibot | Backtrader | Freqtrade | Zipline | Jesse | NautilusTrader | Hummingbot |
|---|---|---|---|---|---|---|---|
| **Mismo código: backtest y real** | Sí | Sí | Sí (cripto) | No | Sí (de pago) | Sí | Sí (cripto) |
| **Acciones** | Sí | Sí | No | Sí | No | Sí | No |
| **Opciones** | **Sí** | No | No | No | No | Limitado | No |
| **Cripto** | Sí | Limitado | Sí | No | Sí | Sí | Sí |
| **Mercados de predicción** | Trading y backtest en Polymarket | No | No | No | No | No | Limitado / no |
| **Futuros** | Sí | Limitado | Solo cripto | Parcial | Solo cripto | Sí | Perpetuos cripto |
| **Runtime de agentes de IA** | Integrado | No | FreqAI (aprendizaje automático) | No | Pipeline de ML | No | Scripts / controladores |
| **Ruta de despliegue gestionado** | BotSpot | No | No | No | Nube de pago | No | Fundación Hummingbot / ecosistema empresarial |

## Brókers admitidos

Alpaca, Interactive Brokers (incluida su API REST), Tradier, Schwab, Tradovate, futuros de TopstepX (a través de ProjectX), Bitunix, trading y backtest de contratos de predicción en Polymarket, y rutas seleccionadas de CCXT para cripto.

LumiBot no afirma dar soporte a todos los exchanges de CCXT. Consulta la documentación para el alcance exacto.

## Documentación completa

Esta página es un resumen de lo esencial del README en inglés. Para el contenido completo, incluidos los ejemplos de equipos de IA, las fuentes de datos, el despliegue y la guía de contribución:

- README en inglés: [README.md](README.md)
- Documentación: https://lumibot.lumiwealth.com/
- Versión gestionada: https://botspot.trade

## Aviso legal

Este software es solo para fines educativos. Operar conlleva riesgo y puedes perder dinero. Los resultados pasados no indican resultados futuros. Este proyecto no ofrece asesoramiento de inversión.
