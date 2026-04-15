#!/usr/bin/env python3
"""
quick_scan.py — 一键分析 CLI 入口
用法:
  ALCHEMY_KEY=xxx python3 quick_scan.py <合约地址>
  ALCHEMY_KEY=xxx python3 quick_scan.py <合约地址> --wechat   # 微信格式
"""
import sys, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from scan_core import run_analysis
from format_wechat import format_wechat_report


def print_full_report(result):
    """终端详细报告"""
    info = result["info"]
    tp = result["token_price"]
    dex_data = result["dex_data"]

    print(f"\n{'='*60}")
    print(f"📊 {info['name']} ({info['symbol']}) 一键分析报告")
    print(f"{'='*60}")
    print(f"  合约: {info.get('contract', '?')}")
    print(f"  总供应: {info['total_supply']:,.0f}")
    print(f"  Owner: {info['owner']}")
    print(f"  LP Pair: {info['pair']}")
    print(f"  价格: ${tp:.8f}")
    if dex_data:
        print(f"  FDV: ${float(dex_data.get('fdv') or 0):,.0f}")
        print(f"  流动性: ${float(dex_data.get('liquidity', {}).get('usd', 0)):,.0f}")
        print(f"  24h量: ${float(dex_data.get('volume', {}).get('h24', 0)):,.0f}")
        print(f"  24h涨跌: {dex_data.get('priceChange', {}).get('h24', 0)}%")
        txns = dex_data.get("txns", {}).get("h24", {})
        print(f"  24h买/卖: {txns.get('buys', 0)}/{txns.get('sells', 0)} 笔")

    # 庄家
    print(f"\n🐋 自动识别 {len(result['whale_addrs'])} 个庄家地址")
    print("-" * 60)
    print(f"\n🐋 庄家成本分析")
    print("-" * 60)
    for w in result["whale_results"]:
        if w["total_buy_amount"] == 0 and w["sell_cnt"] == 0:
            continue
        short = f"{w['addr'][:10]}..{w['addr'][-4:]}"
        icon = "📈" if w["total_pnl"] > 0 else "📉"
        print(f"\n  {short} {w['tag']}")
        print(f"    买 {w['buy_cnt']:>5}笔 {w['total_buy_amount']:>14,.0f} 均价${w['avg_buy']:.6f} 成本${w['total_buy_cost']:>10,.0f}")
        print(f"    卖 {w['sell_cnt']:>5}笔 {w['total_sell_amount']:>14,.0f} 均价${w['avg_sell']:.6f} 收入${w['total_sell_revenue']:>10,.0f}")
        print(f"    持仓: {w['balance']:>14,.0f} (${w['balance']*tp:>10,.0f})")
        print(f"    {icon} 已实现${w['realized']:>+12,.0f} 未实现${w['unrealized']:>+12,.0f} 总${w['total_pnl']:>+12,.0f}")

    # 抛压
    p = result["pressure"]
    print(f"\n💣 抛压预估")
    print("-" * 40)
    print(f"  庄家剩余: {p['remaining']:,.0f} ({p['pct_supply']:.1f}%)")
    print(f"  剩余价值: ${p['remaining_usd']:,.0f}")
    print(f"  全抛价格影响: -{p['impact_pct']:.1f}%")

    # 集中度
    c = result["concentration"]
    print(f"\n📊 持仓集中度")
    print("-" * 40)
    print(f"  Top 5:  {c['top5_pct']:.2f}%")
    print(f"  Top 10: {c['top10_pct']:.2f}%")
    print(f"  Top 20: {c['top20_pct']:.2f}%")
    print(f"  总持仓地址: {result['total_holders']}")

    # Top 20
    print(f"\n🏆 Top 20 持仓")
    print("-" * 60)
    for h in result["top_holders"]:
        short = f"{h['addr'][:10]}..{h['addr'][-4:]}"
        tag = "🐋" if h["is_whale"] else ""
        print(f"  #{h['rank']:2d} {short} | {h['balance']:>13,.0f} ({h['pct']:.2f}%) ${h['usd']:>8,.0f} {tag}")

    # 散户
    r = result["retail"]
    print(f"\n👥 散户分析")
    print("-" * 40)
    print(f"  散户数: {r['count']}")
    print(f"  平均持仓: ${r['avg_usd']:,.0f}")
    print(f"  中位持仓: ${r['median_usd']:,.0f}")
    if r.get("distribution"):
        print(f"  持仓分布:")
        for b, cnt in r["distribution"].items():
            print(f"    {b}: {cnt} 人")

    # 风险
    print(f"\n⚠️ 风险评估")
    print("-" * 40)
    if not result["risks"]:
        print("  ✅ 未发现明显风险")
    for risk in result["risks"]:
        print(f"  ⚠️ {risk}")

    print(f"\n{'='*60}")
    print(f"分析完成 | {result['total_records']:,} 笔转账 | {result['total_holders']} 持仓地址 | {len(result['whale_addrs'])} 庄家")
    print(f"{'='*60}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: ALCHEMY_KEY=xxx python3 quick_scan.py <合约地址> [--wechat]")
        sys.exit(1)

    contract = sys.argv[1]
    wechat_mode = "--wechat" in sys.argv
    deep_mode = "--deep" in sys.argv

    result = run_analysis(contract, deep=deep_mode)

    if "error" in result:
        print(f"❌ {result['error']}")
        sys.exit(1)

    if wechat_mode:
        report = format_wechat_report(
            result["info"], result["token_price"], result["dex_data"],
            result["whale_results"], result["pressure"], result["concentration"],
            result["top_holders"], result["retail"], result["risks"],
            result["total_records"], result["total_holders"], len(result["whale_addrs"]),
            pools=result.get("pools"),
            shard_results=result.get("shard_results"),
            clusters=result.get("clusters"),
            whale_labels=result.get("whale_labels"),
            risk_score=result.get("risk_score"),
            cross_track=result.get("cross_track"),
            fund_trace=result.get("fund_trace"),
            smart_money=result.get("smart_money"),
            smart_money_activity=result.get("smart_money_activity"),
            lp_analysis=result.get("lp_analysis")
        )
        print(report)
    else:
        print_full_report(result)
