#!/usr/bin/env python3
"""
shard_detect.py — 分仓检测模块
追踪庄家转出的子地址，识别分仓→分批出货模式
"""
from collections import defaultdict


def detect_shards(records, whale_addrs, pools_info, total_supply):
    """
    检测庄家分仓行为。
    返回: {
        whale_addr: {
            "shards": [{addr, received, sold, balance, sell_pct}],
            "total_sharded": float,
            "total_shard_sold": float,
            "pattern": str,  # "分仓出货" / "分仓持有" / "无分仓"
        }
    }
    """
    if isinstance(pools_info, str):
        pool_set = {pools_info.lower()}
    else:
        pool_set = {p[0].lower() for p in pools_info}

    ZERO = "0x0000000000000000000000000000000000000000"
    DEAD = "0x000000000000000000000000000000000000dead"
    exclude = {ZERO, DEAD} | pool_set
    whale_set = {a.lower() for a in whale_addrs}

    # 追踪庄家的非LP转出
    whale_transfers_out = defaultdict(lambda: defaultdict(float))
    # 追踪所有地址的 LP 卖出
    addr_lp_sells = defaultdict(float)
    addr_lp_sell_count = defaultdict(int)
    # 追踪所有地址从 LP 买入（预计算，避免 O(N) 内层循环）
    addr_lp_buys = defaultdict(float)
    # 全量 balance
    balances = defaultdict(float)

    for block, fa, ta, amount in records:
        balances[fa] -= amount
        balances[ta] += amount

        # 庄家转出到非LP、非排除地址
        if fa in whale_set and ta not in exclude and ta not in whale_set:
            whale_transfers_out[fa][ta] += amount

        # 任何地址卖到 LP
        if ta in pool_set and fa not in exclude:
            addr_lp_sells[fa] += amount
            addr_lp_sell_count[fa] += 1

        # 任何地址从 LP 买入（预计算）
        if fa in pool_set and ta not in exclude:
            addr_lp_buys[ta] += amount

    results = {}
    threshold = total_supply * 0.001  # 最低 0.1% 才算分仓

    for whale in whale_set:
        transfers = whale_transfers_out.get(whale, {})
        shards = []

        for dst, received in sorted(transfers.items(), key=lambda x: -x[1]):
            if received < threshold:
                continue
            sold = addr_lp_sells.get(dst, 0)
            sell_count = addr_lp_sell_count.get(dst, 0)
            balance = max(balances.get(dst, 0), 0)
            sell_pct = sold / received * 100 if received > 0 else 0

            # 从预计算的 dict 中直接查询，O(1)
            lp_buys = addr_lp_buys.get(dst, 0)

            # 如果从庄家收到的量 > 从LP买的量的2倍，认为是分仓
            if received > lp_buys * 2 or lp_buys == 0:
                shards.append({
                    "addr": dst,
                    "received": received,
                    "sold": sold,
                    "sell_count": sell_count,
                    "balance": balance,
                    "sell_pct": sell_pct,
                    "lp_buys": lp_buys,
                })

        total_sharded = sum(s["received"] for s in shards)
        total_shard_sold = sum(s["sold"] for s in shards)

        if not shards:
            pattern = "无分仓"
        elif total_shard_sold > total_sharded * 0.5:
            pattern = "⚠️分仓出货"
        elif total_shard_sold > total_sharded * 0.1:
            pattern = "分仓部分出货"
        else:
            pattern = "分仓持有"

        results[whale] = {
            "shards": shards[:10],
            "total_sharded": total_sharded,
            "total_shard_sold": total_shard_sold,
            "shard_count": len(shards),
            "pattern": pattern,
        }

    return results


def format_shard_report(shard_results, token_price):
    """格式化分仓检测报告（微信友好）"""
    lines = []
    lines.append("🔗 分仓检测")
    lines.append("")

    has_shards = False
    for whale, data in sorted(shard_results.items(), key=lambda x: -x[1]["total_sharded"]):
        if data["pattern"] == "无分仓":
            continue
        has_shards = True
        short = f"{whale[:6]}..{whale[-4:]}"
        lines.append(f"  {short} [{data['pattern']}]")
        lines.append(f"    分仓{data['shard_count']}个 共{data['total_sharded']:,.0f}枚")
        if data["total_shard_sold"] > 0:
            lines.append(f"    子地址已卖{data['total_shard_sold']:,.0f}枚 (${data['total_shard_sold']*token_price:,.0f})")

        for s in data["shards"][:5]:
            s_short = f"{s['addr'][:6]}..{s['addr'][-4:]}"
            status = ""
            if s["sell_pct"] > 80:
                status = "已出货"
            elif s["sell_pct"] > 20:
                status = "出货中"
            elif s["balance"] > 0:
                status = "持有"
            else:
                status = "已转出"
            lines.append(f"      {s_short} 收{s['received']:,.0f} 卖{s['sold']:,.0f} 余{s['balance']:,.0f} {status}")

    if not has_shards:
        lines.append("  未检测到明显分仓行为")

    return "\n".join(lines)
