#!/usr/bin/env python3
"""一键分析代币
用法: python3 run_analysis.py <合约地址>
"""
import scan_core
import json
import sys

if len(sys.argv) < 2:
    print("用法: python3 run_analysis.py <合约地址>")
    sys.exit(1)

ca = sys.argv[1].strip().lower()
result = scan_core.run_analysis(ca, skip_sync=True)

if "error" in result:
    print("ERROR:", result["error"])
    sys.exit(1)

info = result["info"]
tp = result["token_price"]
dex = result["dex_data"]

print("=" * 60)
print("代币: %s (%s)" % (info["name"], info["symbol"]))
print("合约: %s" % ca)
print("总供应量: {:,.0f}".format(info["total_supply"]))
print("精度: %d" % info["decimals"])
print("LP Pair: %s" % info["pair"])
print("=" * 60)

print("\n价格: $%.8f" % tp)
if dex:
    pc = dex.get("priceChange", {}).get("h24", "?")
    vol = float(dex.get("volume", {}).get("h24", 0))
    liq = float(dex.get("liquidity", {}).get("usd", 0))
    print("24h涨跌: %s%%" % pc)
    print("24h交易量: ${:,.0f}".format(vol))
    print("流动性: ${:,.0f}".format(liq))

print("\n--- 庄家持仓 (Top 10) ---")
for w in result["whale_results"][:10]:
    short = w["addr"][:8] + ".." + w["addr"][-4:]
    tag = " " + w["tag"] if w.get("tag") else ""
    bal = "{:,}".format(int(w["balance"])) if w["balance"] > 0 else "0"
    pnl_pct = w["total_pnl"] / w["total_buy_cost"] * 100 if w["total_buy_cost"] > 0 else 0
    pnl_sign = "+" if pnl_pct > 0 else ""
    print("  %s  买%d笔 卖%d笔 | 持仓%s | 成本$%.6f | PnL%s%.0f%%%s" % (
        short, w["buy_cnt"], w["sell_cnt"], bal, w["avg_buy"], pnl_sign, pnl_pct, tag))

p = result["pressure"]
print("\n--- 抛压评估 ---")
print("  庄家剩余: {:,} ({:.1f}%)".format(int(p["remaining"]), p["pct_supply"]))
print("  庄家剩余USD: ${:,.0f}".format(p["remaining_usd"]))
print("  全抛价格影响: -{:.1f}%".format(p["impact_pct"]))

c = result["concentration"]
print("\n--- 持仓集中度 ---")
print("  Top5: {:.1f}%  Top10: {:.1f}%  Top20: {:.1f}%".format(c["top5_pct"], c["top10_pct"], c["top20_pct"]))

r = result["retail"]
print("\n--- 散户 ---")
print("  总数: {:,} | 平均持仓: ${:.2f}".format(r["count"], r["avg_usd"]))

if result["risks"]:
    print("\n--- 风险 ---")
    for risk in result["risks"]:
        print("  ⚠️ %s" % risk)

print("\n--- 持仓快照 ---")
from snapshot import take_whale_snapshot, format_snapshot
snap = take_whale_snapshot(ca, result["whale_addrs"][:10], tp)
if snap:
    print(format_snapshot(snap))
