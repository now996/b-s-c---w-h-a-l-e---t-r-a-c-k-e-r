#!/usr/bin/env python3
"""
risk_score.py — 综合风险评分系统
把所有分析指标综合成 0-100 的风险分
0 = 极低风险, 100 = 极高风险（大概率跑路盘）
"""


def calculate_risk_score(result):
    """
    输入: run_analysis() 的返回值
    输出: {
        "score": int (0-100),
        "level": str ("低风险"/"中风险"/"高风险"/"极高风险"),
        "emoji": str,
        "breakdown": [{name, score, max, reason}, ...],
    }
    """
    breakdown = []
    info = result["info"]
    tp = result["token_price"]
    dex_data = result.get("dex_data", {})
    whale_results = result["whale_results"]
    pressure = result["pressure"]
    concentration = result["concentration"]
    retail = result["retail"]
    shard_results = result.get("shard_results", {})
    clusters = result.get("clusters", [])
    whale_labels = result.get("whale_labels", {})
    total_supply = info.get("total_supply", 0)

    # ═══ 1. Owner 状态 (0-10) ═══
    owner = info.get("owner", "?")
    if owner == "0x" + "0" * 40:
        s = 0
        reason = "已放弃"
    elif owner == "?":
        s = 5
        reason = "无法确认"
    else:
        s = 10
        reason = "未放弃"
    breakdown.append({"name": "Owner", "score": s, "max": 10, "reason": reason})

    # ═══ 2. 持仓集中度 (0-20) ═══
    top5 = concentration.get("top5_pct", 0)
    if top5 > 60:
        s = 20
        reason = f"Top5={top5:.0f}% 极度集中"
    elif top5 > 40:
        s = 15
        reason = f"Top5={top5:.0f}% 高度集中"
    elif top5 > 25:
        s = 8
        reason = f"Top5={top5:.0f}% 中等集中"
    else:
        s = 3
        reason = f"Top5={top5:.0f}% 分散"
    breakdown.append({"name": "集中度", "score": s, "max": 20, "reason": reason})

    # ═══ 3. 庄家出货程度 (0-20) ═══
    total_whale_buy = sum(w["total_buy_cost"] for w in whale_results)
    total_whale_sell = sum(w["total_sell_revenue"] for w in whale_results)
    sell_ratio = total_whale_sell / total_whale_buy if total_whale_buy > 0 else 0

    cleared = sum(1 for w in whale_results if w["balance"] == 0 and w["total_buy_amount"] > 0)
    dumping = sum(1 for w in whale_results if w.get("tag") == "⚠️出货中")

    if sell_ratio > 1.5:
        s = 20
        reason = f"卖/买={sell_ratio:.1f}x 大量出货"
    elif sell_ratio > 1.0:
        s = 15
        reason = f"卖/买={sell_ratio:.1f}x 净出货"
    elif sell_ratio > 0.5:
        s = 8
        reason = f"卖/买={sell_ratio:.1f}x 部分出货"
    elif sell_ratio > 0.1:
        s = 4
        reason = f"卖/买={sell_ratio:.1f}x 少量出货"
    else:
        s = 0
        reason = "庄家未出货"
    if cleared > len(whale_results) * 0.5:
        s = min(s + 5, 20)
        reason += f" ({cleared}个已清仓)"
    breakdown.append({"name": "庄家出货", "score": s, "max": 20, "reason": reason})

    # ═══ 4. 分仓出货 (0-15) ═══
    shard_dumpers = sum(1 for d in shard_results.values() if "出货" in d.get("pattern", ""))
    total_shard_sold_usd = sum(d.get("total_shard_sold", 0) * tp for d in shard_results.values())

    if shard_dumpers >= 3:
        s = 15
        reason = f"{shard_dumpers}个庄家分仓出货 ${total_shard_sold_usd:,.0f}"
    elif shard_dumpers >= 1:
        s = 8
        reason = f"{shard_dumpers}个庄家分仓出货"
    elif any(d.get("shard_count", 0) > 0 for d in shard_results.values()):
        s = 3
        reason = "有分仓但未出货"
    else:
        s = 0
        reason = "无分仓"
    breakdown.append({"name": "分仓出货", "score": s, "max": 15, "reason": reason})

    # ═══ 5. 庄家团伙 (0-10) ═══
    big_clusters = [c for c in clusters if c["size"] > 2]
    max_cluster = max((c["size"] for c in clusters), default=0)

    if max_cluster > 10:
        s = 10
        reason = f"超大团伙 {max_cluster}个地址"
    elif max_cluster > 5:
        s = 7
        reason = f"大团伙 {max_cluster}个地址"
    elif max_cluster > 2:
        s = 4
        reason = f"小团伙 {max_cluster}个地址"
    else:
        s = 0
        reason = "无团伙"
    breakdown.append({"name": "庄家团伙", "score": s, "max": 10, "reason": reason})

    # ═══ 6. 抛压 (0-15) ═══
    impact = pressure.get("impact_pct", 0)
    pct_supply = pressure.get("pct_supply", 0)

    if impact > 80:
        s = 15
        reason = f"全抛影响-{impact:.0f}% 致命"
    elif impact > 50:
        s = 10
        reason = f"全抛影响-{impact:.0f}% 严重"
    elif impact > 20:
        s = 5
        reason = f"全抛影响-{impact:.0f}% 中等"
    else:
        s = 2
        reason = f"全抛影响-{impact:.0f}% 可控"
    breakdown.append({"name": "抛压", "score": s, "max": 15, "reason": reason})

    # ═══ 7. 流动性 (0-10) ═══
    liq = float(dex_data.get("liquidity", {}).get("usd", 0)) if dex_data else 0
    if liq < 10000:
        s = 10
        reason = f"池子${liq:,.0f} 极低"
    elif liq < 50000:
        s = 7
        reason = f"池子${liq:,.0f} 偏低"
    elif liq < 200000:
        s = 4
        reason = f"池子${liq:,.0f} 中等"
    elif liq < 1000000:
        s = 2
        reason = f"池子${liq:,.0f} 充足"
    else:
        s = 0
        reason = f"池子${liq:,.0f} 深厚"
    breakdown.append({"name": "流动性", "score": s, "max": 10, "reason": reason})

    # ═══ 8. LP 安全 (0-10) ═══
    lp_analysis = result.get("lp_analysis", {})
    if lp_analysis:
        lp_risk = lp_analysis.get("risk_level", "low")
        provider_count = lp_analysis.get("provider_count", 0)
        remove_pct = lp_analysis.get("remove_pct", 0)
        is_locked = lp_analysis.get("is_locked", False)
        is_burned = lp_analysis.get("is_burned", False)

        s = 0
        reasons = []
        # LP 提供者数量
        if provider_count <= 1:
            s += 5
            reasons.append("单LP提供者")
        elif provider_count <= 2:
            s += 3
            reasons.append(f"仅{provider_count}个LP")
        # 撤池比例
        if remove_pct > 50:
            s += 4
            reasons.append(f"已撤{remove_pct:.0f}%")
        elif remove_pct > 20:
            s += 2
            reasons.append(f"撤{remove_pct:.0f}%")
        # LP 锁定/销毁（正面信号，降分）
        if is_locked or is_burned:
            s = max(s - 3, 0)
            reasons.append("LP已锁定" if is_locked else "LP已销毁")

        s = min(s, 10)
        reason = " | ".join(reasons) if reasons else "LP安全"
        breakdown.append({"name": "LP安全", "score": s, "max": 10, "reason": reason})
    else:
        breakdown.append({"name": "LP安全", "score": 5, "max": 10, "reason": "未检测"})

    # ═══ 9. 跨合约前科 (0-10, 仅 deep 模式) ═══
    cross_track = result.get("cross_track", {})
    if cross_track:
        criminals = sum(1 for d in cross_track.values() if "惯犯" in d.get("pattern", ""))
        max_tokens = max((d.get("token_count", 0) for d in cross_track.values()), default=0)
        if criminals >= 3:
            s = 10
            reason = f"{criminals}个惯犯 最多参与{max_tokens}个token"
        elif criminals >= 1:
            s = 6
            reason = f"{criminals}个惯犯"
        else:
            s = 0
            reason = "无前科"
        breakdown.append({"name": "前科", "score": s, "max": 10, "reason": reason})

    # 总分
    total_score = sum(b["score"] for b in breakdown)
    total_max = sum(b["max"] for b in breakdown)
    # 归一化到 0-100
    score = int(total_score / total_max * 100) if total_max > 0 else 50

    if score >= 75:
        level = "极高风险"
        emoji = "🔴"
    elif score >= 50:
        level = "高风险"
        emoji = "🟠"
    elif score >= 30:
        level = "中风险"
        emoji = "🟡"
    else:
        level = "低风险"
        emoji = "🟢"

    return {
        "score": score,
        "level": level,
        "emoji": emoji,
        "breakdown": breakdown,
    }


def format_risk_score(risk):
    """格式化风险评分（微信友好）"""
    lines = []
    lines.append(f"{risk['emoji']} 风险评分: {risk['score']}/100 [{risk['level']}]")
    for b in risk["breakdown"]:
        bar_len = 8
        filled = int(b["score"] / b["max"] * bar_len) if b["max"] > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        lines.append(f"  {b['name']}: {bar} {b['score']}/{b['max']} {b['reason']}")
    return "\n".join(lines)
