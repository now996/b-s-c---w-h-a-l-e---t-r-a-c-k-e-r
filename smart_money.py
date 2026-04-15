#!/usr/bin/env python3
"""
smart_money.py — 聪明钱识别
从链上数据中找出早期买入且盈利的地址
用途：反向利用，跟踪聪明钱的新动作
"""
import os
import sys
import time
import requests
from collections import defaultdict

ALCHEMY_KEY = os.environ.get("ALCHEMY_KEY", "")
ALCHEMY_URL = f"https://bnb-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"

ZERO = "0x0000000000000000000000000000000000000000"
DEAD = "0x000000000000000000000000000000000000dead"


def find_smart_money(records, pools_info, total_supply, token_price, block_to_price_fn, whale_addrs=None):
    """
    从转账记录中找出聪明钱。
    聪明钱特征：
    1. 早期买入（前20%的区块）
    2. 已实现盈利（卖出均价 > 买入均价）
    3. 不是庄家/项目方（排除已知庄家）
    
    返回: [{
        addr, buy_amount, sell_amount, balance,
        avg_buy_price, avg_sell_price, realized_pnl, unrealized_pnl,
        total_pnl, roi, entry_block, is_early, score
    }]
    """
    if isinstance(pools_info, str):
        pool_set = {pools_info.lower()}
    else:
        pool_set = {p[0].lower() for p in pools_info}

    exclude = {ZERO, DEAD} | pool_set
    whale_set = {a.lower() for a in (whale_addrs or [])}

    if not records:
        return []

    # 确定"早期"的界限（前20%的区块范围）
    min_block = records[0][0]
    max_block = records[-1][0]
    early_cutoff = min_block + (max_block - min_block) * 0.2

    # 统计每个地址的买卖
    addr_stats = defaultdict(lambda: {
        "buys": [], "sells": [],
        "first_buy_block": float("inf"),
        "balance": 0,
    })

    balances = defaultdict(float)
    for block, fa, ta, amount in records:
        balances[fa] -= amount
        balances[ta] += amount

        if fa in pool_set and ta not in exclude:
            # 买入
            price = block_to_price_fn(block)
            addr_stats[ta]["buys"].append((amount, price, block))
            addr_stats[ta]["first_buy_block"] = min(addr_stats[ta]["first_buy_block"], block)

        elif ta in pool_set and fa not in exclude:
            # 卖出
            price = block_to_price_fn(block)
            addr_stats[fa]["sells"].append((amount, price, block))

    # 筛选聪明钱
    smart_money = []

    for addr, stats in addr_stats.items():
        if addr in exclude or addr in whale_set:
            continue
        if not stats["buys"]:
            continue

        total_buy_amount = sum(a for a, _, _ in stats["buys"])
        total_buy_cost = sum(a * p for a, p, _ in stats["buys"])
        total_sell_amount = sum(a for a, _, _ in stats["sells"])
        total_sell_revenue = sum(a * p for a, p, _ in stats["sells"])

        if total_buy_amount < total_supply * 0.0001:
            continue  # 太小的忽略

        avg_buy = total_buy_cost / total_buy_amount if total_buy_amount > 0 else 0
        avg_sell = total_sell_revenue / total_sell_amount if total_sell_amount > 0 else 0
        balance = max(balances.get(addr, 0), 0)

        realized = total_sell_revenue - (total_sell_amount * avg_buy) if total_sell_amount > 0 and avg_buy > 0 else 0
        unrealized = balance * (token_price - avg_buy) if balance > 0 and avg_buy > 0 else 0
        total_pnl = realized + unrealized
        roi = total_pnl / total_buy_cost * 100 if total_buy_cost > 0 else 0

        is_early = stats["first_buy_block"] <= early_cutoff

        # 聪明钱评分
        score = 0
        if is_early:
            score += 30  # 早期买入
        if total_pnl > 0:
            score += 20  # 盈利
        if roi > 100:
            score += 20  # 高ROI
        elif roi > 50:
            score += 10
        if total_sell_amount > total_buy_amount * 0.3:
            score += 15  # 懂得止盈
        if balance > 0 and total_sell_amount > 0:
            score += 15  # 卖了一部分还留着

        if score >= 40:  # 至少40分才算聪明钱
            smart_money.append({
                "addr": addr,
                "buy_amount": total_buy_amount,
                "sell_amount": total_sell_amount,
                "balance": balance,
                "buy_cost": total_buy_cost,
                "sell_revenue": total_sell_revenue,
                "avg_buy": avg_buy,
                "avg_sell": avg_sell,
                "realized": realized,
                "unrealized": unrealized,
                "total_pnl": total_pnl,
                "roi": roi,
                "is_early": is_early,
                "first_buy_block": stats["first_buy_block"],
                "score": score,
            })

    smart_money.sort(key=lambda x: -x["score"])
    return smart_money[:20]


def track_smart_money_activity(smart_addrs, alchemy_key, max_addrs=5, max_pages=1):
    """
    追踪聪明钱最近在买什么新币。
    返回: {addr: [{token, action, amount, count}]}
    """
    results = {}

    for i, addr in enumerate(smart_addrs[:max_addrs]):
        print(f"  追踪聪明钱 {i+1}/{min(len(smart_addrs), max_addrs)}: {addr[:10]}...", file=sys.stderr)

        url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
        # 查最近的 ERC20 买入（从 LP 收到 token）
        try:
            r = requests.post(url, json={
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [{
                    "toAddress": addr,
                    "category": ["erc20"],
                    "maxCount": "0x64",
                    "order": "desc",
                    "withMetadata": True,
                }],
                "id": 1
            }, timeout=30)
            data = r.json()
            transfers = (data.get("result") or {}).get("transfers") or []

            # 统计最近买了哪些 token
            token_buys = defaultdict(lambda: {"count": 0, "amount": 0})
            for tx in transfers:
                raw = tx.get("rawContract") or {}
                token = (raw.get("address") or "").lower()
                val = float(tx.get("value") or 0)
                if token and val > 0:
                    token_buys[token]["count"] += 1
                    token_buys[token]["amount"] += val

            # 排序
            recent = sorted(token_buys.items(), key=lambda x: -x[1]["count"])
            results[addr] = [{"token": t, "count": d["count"], "amount": d["amount"]} for t, d in recent[:5]]

        except Exception as e:
            print(f"  error: {e}", file=sys.stderr)
            results[addr] = []

        time.sleep(0.5)

    return results


def format_smart_money(smart_money, token_price, recent_activity=None):
    """格式化聪明钱报告"""
    lines = []
    lines.append("🧠 聪明钱")

    if not smart_money:
        lines.append("  未发现符合条件的聪明钱")
        return "\n".join(lines)

    lines.append(f"  发现 {len(smart_money)} 个聪明钱地址")
    lines.append("")

    for sm in smart_money[:8]:
        short = f"{sm['addr'][:6]}..{sm['addr'][-4:]}"
        pnl_icon = "📈" if sm["total_pnl"] > 0 else "📉"
        early_tag = "🌅早期" if sm["is_early"] else ""
        profit_tag = ""
        if sm["roi"] > 200:
            profit_tag = "🔥暴赚"
        elif sm["roi"] > 50:
            profit_tag = "💰盈利"
        elif sm["roi"] > 0:
            profit_tag = "📈微赚"

        lines.append(f"  {short} 评分{sm['score']} {early_tag} {profit_tag}")
        lines.append(f"    买${sm['buy_cost']:,.0f} 卖${sm['sell_revenue']:,.0f} 持${sm['balance']*token_price:,.0f}")
        lines.append(f"    {pnl_icon} 盈亏${sm['total_pnl']:+,.0f} ROI {sm['roi']:+.0f}%")

    # 聪明钱最近在买什么
    if recent_activity:
        # 排除常见 token
        IGNORE = {
            "0x55d398326f99059ff775485246999027b3197955",  # USDT
            "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
            "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
            "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH
        }
        lines.append("")
        lines.append("  📡 聪明钱最近在买:")
        all_tokens = defaultdict(int)
        for addr, tokens in recent_activity.items():
            for t in tokens:
                if t["token"].lower() not in IGNORE:
                    all_tokens[t["token"]] += t["count"]
        top_tokens = sorted(all_tokens.items(), key=lambda x: -x[1])[:5]
        if top_tokens:
            for token, count in top_tokens:
                t_short = f"{token[:8]}..{token[-4:]}"
                lines.append(f"    {t_short} ({count}笔)")
        else:
            lines.append("    无新 meme 币动作")

    return "\n".join(lines)
