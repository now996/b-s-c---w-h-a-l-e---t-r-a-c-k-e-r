#!/usr/bin/env python3
"""
labeler.py — 庄家地址自动标签系统
根据链上行为特征自动给地址打标签
"""
from collections import defaultdict


def label_whales(records, whale_addrs, pools_info, total_supply, shard_results=None, clusters=None):
    """
    给每个庄家地址打标签。
    返回: {addr: [label1, label2, ...]}
    
    标签类型：
    - 🏦项目方: 从零地址/合约大量收到token，未通过LP买入
    - 🤖做市商: 高频买卖，买卖笔数都很多
    - 💰大买家: 主要通过LP大量买入
    - 🔀分仓号: 从其他庄家收到token，自己不从LP买
    - 📦囤币: 只买不卖，长期持有
    - 🏃跑路: 已全部卖出或转出
    - ⚠️出货中: 卖出量超过买入量的50%
    - 🐋主力: 团伙中持仓/买入最大的地址
    """
    if isinstance(pools_info, str):
        pool_set = {pools_info.lower()}
    else:
        pool_set = {p[0].lower() for p in pools_info}

    ZERO = "0x0000000000000000000000000000000000000000"
    exclude = {ZERO, "0x000000000000000000000000000000000000dead"} | pool_set
    whale_set = {a.lower() for a in whale_addrs}

    # 统计每个地址的行为
    stats = {}
    balances = defaultdict(float)

    for addr in whale_set:
        stats[addr] = {
            "lp_buy_amount": 0, "lp_buy_count": 0,
            "lp_sell_amount": 0, "lp_sell_count": 0,
            "recv_from_zero": 0, "recv_from_whale": 0,
            "recv_from_other": 0, "sent_to_whale": 0,
            "sent_to_other": 0,
        }

    for block, fa, ta, amount in records:
        balances[fa] -= amount
        balances[ta] += amount

        if ta in whale_set:
            if fa in pool_set:
                stats[ta]["lp_buy_amount"] += amount
                stats[ta]["lp_buy_count"] += 1
            elif fa == ZERO:
                stats[ta]["recv_from_zero"] += amount
            elif fa in whale_set:
                stats[ta]["recv_from_whale"] += amount
            elif fa not in exclude:
                stats[ta]["recv_from_other"] += amount

        if fa in whale_set:
            if ta in pool_set:
                stats[fa]["lp_sell_amount"] += amount
                stats[fa]["lp_sell_count"] += 1
            elif ta in whale_set:
                stats[fa]["sent_to_whale"] += amount
            elif ta not in exclude:
                stats[fa]["sent_to_other"] += amount

    # 打标签
    labels = {}
    for addr in whale_set:
        s = stats[addr]
        bal = max(balances.get(addr, 0), 0)
        tags = []

        total_received = s["lp_buy_amount"] + s["recv_from_zero"] + s["recv_from_whale"] + s["recv_from_other"]

        # 项目方：从零地址大量收到，或者不通过LP买入但持仓大
        if s["recv_from_zero"] > total_supply * 0.01:
            tags.append("🏦项目方")
        elif s["lp_buy_amount"] == 0 and total_received > total_supply * 0.01:
            if s["recv_from_whale"] > s["recv_from_other"]:
                tags.append("🔀分仓号")
            else:
                tags.append("🏦项目方")

        # 做市商：高频买卖
        if s["lp_buy_count"] > 50 and s["lp_sell_count"] > 50:
            tags.append("🤖做市商")
        elif s["lp_buy_count"] > 20 and s["lp_sell_count"] > 20:
            ratio = s["lp_sell_amount"] / s["lp_buy_amount"] if s["lp_buy_amount"] > 0 else 0
            if 0.3 < ratio < 3:
                tags.append("🤖做市商")

        # 大买家
        if s["lp_buy_amount"] > total_supply * 0.01 and "🤖做市商" not in tags and "🏦项目方" not in tags:
            tags.append("💰大买家")

        # 状态标签
        if bal == 0 and total_received > 0:
            tags.append("🏃已跑路")
        elif s["lp_sell_amount"] > s["lp_buy_amount"] * 0.8 and s["lp_sell_amount"] > 0:
            tags.append("⚠️出货中")
        elif s["lp_sell_amount"] == 0 and s["lp_buy_amount"] > 0 and bal > 0:
            tags.append("📦囤币")
        elif s["lp_sell_amount"] == 0 and s["lp_buy_amount"] == 0 and bal > 0:
            tags.append("📦持仓")

        # 分仓检测标签
        if shard_results and addr in shard_results:
            sd = shard_results[addr]
            if "出货" in sd["pattern"]:
                if "⚠️出货中" not in tags:
                    tags.append("⚠️分仓出货")
            elif sd["shard_count"] > 0:
                tags.append(f"🔗分仓{sd['shard_count']}个")

        if not tags:
            tags.append("❓未知")

        labels[addr] = tags

    # 主力标签：团伙中买入最大的
    if clusters:
        for c in clusters:
            if c["size"] <= 1:
                continue
            members = c["members"]
            # 找买入最大的
            max_buy = 0
            max_addr = None
            for addr in members:
                if addr in stats:
                    buy = stats[addr]["lp_buy_amount"]
                    if buy > max_buy:
                        max_buy = buy
                        max_addr = addr
            if max_addr and max_addr in labels:
                labels[max_addr].insert(0, "🐋主力")

    return labels
