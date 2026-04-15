"""
format_wechat.py — 把分析结果格式化为微信友好的简洁报告
"""

def format_wechat_report(info, tp, dex_data, whale_results, pressure, concentration, top_holders, retail, risks, total_records, total_holders, total_whales, pools=None, shard_results=None, clusters=None, whale_labels=None, risk_score=None, cross_track=None, fund_trace=None, smart_money=None, smart_money_activity=None, lp_analysis=None):
    """生成微信友好的简洁报告"""
    lines = []

    # 标题
    lines.append(f"🔍 {info['name']}({info['symbol']}) 扒庄报告")
    lines.append("")

    # 风险评分（放最前面）
    if risk_score:
        from risk_score import format_risk_score
        lines.append(format_risk_score(risk_score))
        lines.append("")

    # 基本面
    lines.append(f"💰 价格 ${tp:.6f}")
    if dex_data:
        fdv = float(dex_data.get('fdv') or 0)
        liq = float(dex_data.get('liquidity', {}).get('usd', 0))
        vol = float(dex_data.get('volume', {}).get('h24', 0))
        chg = dex_data.get('priceChange', {}).get('h24', 0)
        txns = dex_data.get("txns", {}).get("h24", {})
        lines.append(f"市值 ${fdv:,.0f} | 池子 ${liq:,.0f}")
        lines.append(f"24h量 ${vol:,.0f} | 涨跌 {chg}%")
        lines.append(f"24h买{txns.get('buys',0)}笔 卖{txns.get('sells',0)}笔")
    lines.append(f"Owner: {'已放弃✅' if info['owner'] == '0x' + '0'*40 else '未放弃⚠️'}")

    # LP 池子
    if pools and len(pools) > 0:
        pool_tags = [f"{v}" for _, v in pools]
        lines.append(f"LP: {' + '.join(pool_tags)}")
    lines.append("")

    # 持仓集中度
    lines.append(f"📊 集中度")
    lines.append(f"Top5 {concentration['top5_pct']:.1f}% | Top10 {concentration['top10_pct']:.1f}% | Top20 {concentration['top20_pct']:.1f}%")
    lines.append(f"持仓地址 {total_holders} 个")
    lines.append("")

    # 庄家（只列最重要的几个）
    lines.append(f"🐋 庄家 ({total_whales}个)")
    for w in whale_results[:8]:
        if w["total_buy_amount"] == 0 and w["sell_cnt"] == 0:
            continue
        short = f"{w['addr'][:6]}..{w['addr'][-4:]}"
        bal = w["balance"]
        pnl = w["total_pnl"]
        pnl_icon = "📈" if pnl > 0 else "📉"

        # 标签
        tags = ""
        if whale_labels and w["addr"] in whale_labels:
            tags = " ".join(whale_labels[w["addr"]])

        status = w.get("tag", "")
        if not status:
            if bal == 0:
                status = "已清仓"
            elif w["sell_cnt"] > w["buy_cnt"] * 0.5:
                status = "出货中"

        buy_usd = w["total_buy_cost"]
        sell_usd = w["total_sell_revenue"]
        bal_usd = bal * tp

        line = f"  {short} {tags or status}"
        line += f"\n    买${buy_usd:,.0f} 卖${sell_usd:,.0f}"
        if bal > 0:
            line += f" 持${bal_usd:,.0f}"
        if pnl != 0:
            line += f"\n    {pnl_icon}盈亏${pnl:+,.0f}"
        lines.append(line)
    lines.append("")

    # 抛压
    lines.append(f"💣 抛压")
    lines.append(f"庄家剩余 {pressure['remaining']:,.0f}枚 ({pressure['pct_supply']:.1f}%)")
    lines.append(f"价值 ${pressure['remaining_usd']:,.0f}")
    lines.append(f"全抛影响 -{pressure['impact_pct']:.1f}%")
    lines.append("")

    # LP 分析
    if lp_analysis and lp_analysis.get("total_adds", 0) > 0:
        from lp_detect import format_lp_report
        lp_text = format_lp_report(lp_analysis)
        lines.append(lp_text)
        lines.append("")

    # 散户
    lines.append(f"👥 散户 {retail['count']}人")
    lines.append(f"平均${retail.get('avg_usd',0):,.0f} 中位${retail.get('median_usd',0):,.0f}")
    if retail.get("distribution"):
        dist = retail["distribution"]
        lines.append(f"<$100:{dist.get('<$100',0)} $100-1k:{dist.get('$100-1k',0)} $1k+:{dist.get('$1k-10k',0)+dist.get('$10k-100k',0)+dist.get('>$100k',0)}")
    lines.append("")

    # 分仓检测
    if shard_results:
        from shard_detect import format_shard_report
        shard_text = format_shard_report(shard_results, tp)
        lines.append(shard_text)
        lines.append("")

    # 庄家关联
    if clusters:
        from cluster import format_cluster_report
        cluster_text = format_cluster_report(clusters)
        lines.append(cluster_text)
        lines.append("")

    # 跨合约追踪
    if cross_track:
        from cross_track import format_cross_track
        cross_text = format_cross_track(cross_track, tp)
        lines.append(cross_text)
        lines.append("")

    # 资金溯源
    if fund_trace:
        from fund_trace import format_fund_trace
        fund_text = format_fund_trace(fund_trace)
        lines.append(fund_text)
        lines.append("")

    # 聪明钱
    if smart_money:
        from smart_money import format_smart_money
        smart_text = format_smart_money(smart_money, tp, smart_money_activity)
        lines.append(smart_text)
        lines.append("")

    # 风险
    if risks:
        lines.append("⚠️ 风险")
        for r in risks:
            lines.append(f"  {r}")
    else:
        lines.append("✅ 未发现明显风险")

    lines.append("")
    lines.append(f"📋 数据: {total_records:,}笔链上转账")

    return "\n".join(lines)
