# Pelosi stock bot, switch window v1 (May 4 to Aug 28, 2026): FAILED

Run `pelosi-stocks-switches` (WAVE99), GPT-6 Luna, Yahoo, $100,000. Graded with
`grade_holdings.py` against her filings: 2024 yearly report (filed 5/15/2025),
2025 yearly report (filed 5/15/2026), the 1/23/2026 trade report, 6/23/2026
(INTC and UBER calls only), and 8/21/2026 (BE and INTC shares and calls).

What worked
- Day one bought 18 of her stocks; every order filled.
- Picked up the 5/15 yearly report on 5/18 (first trading day after it was filed) and rebalanced (sold 65 AAPL, added AVGO, V, WBD).
- Did not trade on quiet days.

What failed
- Never held AB, DIS, PYPL, TEM or VST, which she still holds. The researcher treated her partial DIS and PYPL sales as full sales, skipped AB because it is a partnership, and left out shares from exercised calls (AVGO before 5/15, TEM, VST).
- Sold all PANW on 6/23 after a re-read of the yearly report missed its range, then bought 6 back on 7/8.
- Missed the 8/21 report (BE, INTC): it compared only the newest yearly report and answered NOTHING NEW.
- Memory notes keep only the start of each answer, so "end with the newest report date" was cut off and the bot re-reported the same filings on several days.

Fix (research prompt, both bots): check the filing list every day; start with the
newest filing of any kind; a sale removes a stock only when the report says the
whole position was sold; exercised calls are stock buys; partnership units with a
ticker count as stocks. Rerun as WAVE100.
