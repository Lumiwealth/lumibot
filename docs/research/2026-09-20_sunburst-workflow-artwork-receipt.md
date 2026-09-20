# Sunburst workflow artwork receipt

Date: 2026-09-20

All fourteen assets below are independent, raw approved-generator outputs. They
were copied into the documentation without cropping, retouching, compositing,
overlays, or text repair and were inspected at full resolution.

## Generation contract

- Model: GPT Image 2.5 Sunburst
- Quality: maximum
- Purpose: website/documentation
- Shared prompt: Create a clean, friendly LumiBot workflow diagram on a light
  white-to-pale-blue background using the supplied Spot mascot as the character
  language. Use a wide composition, large readable typography, one straight
  horizontal flow, very few role labels, and only the arrows needed to show the
  real code topology. Do not add charts, decorative finance icons, crossing
  arrows, tiny labels, invented agents, or dense explanatory text. The final
  trading or portfolio owner must be visually obvious. Render every label in
  the supplied asset-specific contract exactly.

The following asset-specific prompt contracts record the exact topology and
visible labels used for regeneration. The generator invocation also referenced
the previous asset and/or the canonical Spot close-up where useful.

| Asset | Asset-specific prompt contract | Raw output | SHA-256 |
|---|---|---|---|
| `ai-congress-disclosures.png` | `Congress Disclosure Strategy`; subtitle `Public first. Research second. Risk decides.`; `Public Disclosure` -> `Research Agent` -> `Trading & Risk` -> `LumiBot`. | `.image_generator_output/image_20260920_172253_570345_1.png` | `9dc423d24e7adbb9ce59fface65bcf85ef9cc11ced4c29784724a95f38f05eba` |
| `ai-sec-insider-filings.png` | `SEC Insider Filings`; subtitle `Public filing. Correct classification. Risk decides.`; `SEC Form 4` -> `Research Agent` -> `Trading & Risk` -> `LumiBot`. | `.image_generator_output/image_20260920_172535_191483_1.png` | `82add84230c36659c28c63390c5b538613c03514d93df56af257df600c0d5da6` |
| `ai-browser-research-showcase.png` | `Browser Research Showcase`; subtitle `Log in. Research. Trade. Share proof.`; `Authorized Portal` -> `Browser Research` -> `Trading & Risk` -> `Optional Publisher`. | `.image_generator_output/image_20260920_172435_706858_1.png` | `62062d5628845cd319f98240a1753869783835795181cc5d4c36f3c7fdd2a09d` |
| `ai-credit-spread.png` | `AI Credit Spread`; subtitle `One agent analyzes, sizes, and executes.`; `Market Data` -> `Credit Spread Agent` -> `LumiBot`. | `.image_generator_output/image_20260920_172634_272345_1.png` | `4e1ac9cf4ffcc00c4ab42440b231b8076119cf49b3602b6a1df6b595b813a0ca` |
| `ai-iron-condor.png` | `AI Iron Condor`; subtitle `One agent finds, checks, and manages the spread.`; `Option Chain` -> `Iron Condor Agent` -> `LumiBot`. | `.image_generator_output/image_20260920_172731_475506_1.png` | `639a3bf19a14c9566002609682c210a115ef47f2c1d4a161415e436b7d010fd1` |
| `ai-opening-range-breakout.png` | `AI Opening Range Breakout`; subtitle `One agent watches the range and manages the trade.`; `Opening Range` -> `ORB Agent` -> `LumiBot`. | `.image_generator_output/image_20260920_173055_767798_1.png` | `e929328cff462b76709b12a5592dbf04a9b8d88b4a987afe651f540976006dca` |
| `ai-vwap.png` | `AI VWAP Strategy`; subtitle `One agent compares price, volume, and risk.`; `Price + Volume` -> `VWAP Agent` -> `LumiBot`. | `.image_generator_output/image_20260920_172958_690233_1.png` | `1e405a3e97fd0f8654164698cdf5206f5b8d003e5ba44a1cbac389b88ab5b932` |
| `ai-spx-zero-dte-bear-call-team.png` | `SPX 0DTE Bear Call Team`; subtitle `Research finds the setup. Trading owns risk.`; `SPX Market` -> `Research Agent` -> `Trading Agent` -> `LumiBot`. | `.image_generator_output/image_20260920_172903_446080_1.png` | `ab74d18a0471137303e6c7aa4d2bfc91425e6dbf9f23b4936c5ca679be32d118` |
| `bill-ackman-concentrated.png` | `Concentrated Quality Team`; subtitle `Find quality. Build conviction. Attack the thesis. Decide.`; `Quality Research` -> `Activist Bull` -> `Short Seller` -> `Portfolio Manager`. | `.image_generator_output/image_20260920_173150_319531_1.png` | `d115acd09e12d8663b520e06f806fe88d57d6eb7802c0b3b30150467fe9af39b` |
| `bull-bear-large-cap-stocks.png` | `Bull vs Bear Large-Cap Stocks`; subtitle `Research. Debate. One trading decision.`; `Research` -> `Bull Case` -> `Bear Case` -> `Trader`. | `.image_generator_output/image_20260920_173319_994258_1.png` | `50267f7b324bc37a2859cb5406982458e3b1dcdaa384a486f3d3fb747015640b` |
| `bull-bear-leveraged-etf.png` | `Bull vs Bear: Leveraged ETFs`; subtitle `Research. Debate. One trading decision.`; `ETF Research` -> `Bull Case` -> `Bear Case` -> `Trader`. | `.image_generator_output/image_20260920_173511_439165_1.png` | `701d80d1a6b3cce20539147d5c031c545beaf223d3d20f7e71a42c35321ce519` |
| `warren-buffett-value.png` | `Long-Term Value Team`; subtitle `Read the filings. Demand value. Decide.`; `Report Reader` -> `Valuation Skeptic` -> `Portfolio Manager`. | `.image_generator_output/image_20260920_173416_190902_1.png` | `c6cb5fc1fbbb1eb8ad198b06506e255828a91a85e7b948859bc5c821be96015b` |
| `ray-dalio-idea-meritocracy.png` | `Macro Idea Meritocracy`; subtitle `Build the view. Challenge it. Trade once.`; `Growth` -> `Inflation` -> `Debt & Liquidity` -> `Disagreement` -> `Trader`. | `.image_generator_output/image_20260920_173609_995618_1.png` | `08880ebef98df090d1bd185e38ed521ba3b416069eb7361659ba93860104a560` |
| `citadel-sector-pods.png` | `Sector Pod Team`; subtitle `Five specialists. One risk review. One portfolio decision.`; `5 Sector Pods` -> `Risk Manager` -> `Portfolio Manager`; exactly two arrows. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-9f45a615-0b90-4359-b99a-23dda0fb4ebc.png` | `3316eb0df31351353f4b4e691ea08404ad586f2e90f307619a6a80d4bf235d85` |

The first thirteen assets are 1536 x 864. The final sector-pod asset is 1672 x
941. Dimensions are generator-controlled and were not altered.
