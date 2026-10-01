import csv, glob, sys
from collections import defaultdict
from pathlib import Path
D = Path(__file__).resolve().parents[1] / "2026-09-23-ai-strategy-backtests"
A=set("AAPL AB AMZN AVGO AXP CLNE CMCSA CRM CRWD DBX DIS GOOGL IBKR MORN MSFT NFLX NVDA PANW PYPL QCOM RBLX SQ T TEM V VST WBD".split())
B=set(A)            # 2025 yearly (5/15/2026) + Jan 2026 exercises: same names, new weights
Dset=B|{"BE","INTC"}   # 8/21/2026 report adds Bloom Energy and Intel shares
checks=[("2026-05-15","before 5/15 yearly report",A),("2026-06-22","after 5/15 yearly report",B),("2026-08-20","after 6/23 calls report",B),("2026-08-28","after 8/21 BE/INTC report",Dset)]
def run(name):
    f=glob.glob(f"{D}/{name}_2*_trades.csv")
    if not f: print(name,"no trades file yet"); return
    rows=[r for r in csv.DictReader(open(sorted(f)[-1])) if r["status"]=="fill"]
    print(f"== {name}: {len(rows)} fills")
    for day,label,expect in checks:
        pos=defaultdict(float)
        for r in rows:
            if r["time"][:10]>day: continue
            key=r["symbol"] if r["asset.asset_type"]=="stock" else f"{r['symbol']} {r['asset.right']} {r['asset.strike']} {r['asset.expiration']}"
            q=float(r["filled_quantity"] or 0); pos[key]+= q if r["side"].startswith("buy") else -q
        held={k for k,v in pos.items() if abs(v)>1e-9}
        stocks={k for k in held if " " not in k}; opts=sorted(k for k in held if " " in k)
        print(f" {day} {label}: {len(stocks)} stocks; missing {sorted(expect-stocks)}; extra {sorted(stocks-expect)}; options {opts}")
    trades=defaultdict(list)
    for r in rows: trades[r["time"][:10]].append(f"{r['side']} {float(r['filled_quantity']):g} {r['symbol']}{'' if r['asset.asset_type']=='stock' else ' '+r['asset.right']+' '+r['asset.strike']+' '+r['asset.expiration']}")
    for d in sorted(trades): print("  ",d,"; ".join(trades[d])[:600])
for n in sys.argv[1:]: run(n)
