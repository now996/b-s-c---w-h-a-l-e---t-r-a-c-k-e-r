#!/usr/bin/env python3
"""
rhythm.py — 庄家操盘节奏分析
分析庄家的买卖时间规律、拉盘/砸盘周期、活跃时段
用法:
  python3 rhythm.py                          # 分析 config 中所有合约
  python3 rhythm.py <合约地址>               # 分析指定合约
"""
import sqlite3
import json
import sys
import os
import requests
from collections import defaultdict
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "whale_tracker.db")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_block_ts_mapping():
    path = os.path.join(DATA_DIR, "block_ts_mapping.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def load_price_history():
    path = os.path.join(DATA_DIR, "price_history.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        candles = json.load(f)
    price_map = {}
    for c in candles:
        vwap = (c["o"] + c["h"] + c["l"] + c["c"]) / 4
        price_map[c["ts"]] = vwap
    return price_map


def block_to_ts(block, slope, intercept):
    return slope * block + intercept


def block_to_price(block, slope, intercept, price_map):
    ts = block_to_ts(block, slope, intercept)
    hour_ts = int(ts // 3600) * 3600
    if hour_ts in price_map:
        return price_map[hour_ts]
    for delta in range(1, 5):
        if hour_ts + delta * 3600 in price_map:
            return price_map[hour_ts + delta * 3600]
        if hour_ts - delta * 3600 in price_map:
            return price_map[hour_ts - delta * 3600]
    return 0


def get_token_price(contract):
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{contract}", timeout=10)
        pairs = r.json().get("pairs", [])
        if pairs:
            return float(pairs[0].get("priceUsd") or 0)
    except Exception:
        pass
    return 0


def analyze_rhythm(contract, pair, whale_set, name="Token"):
    """分析单个合约的庄家操盘节奏，返回格式化文本"""
    bmap = load_block_ts_mapping()
    if not bmap:
        return f"[rhythm] 缺少 block_ts_mapping.json，跳过 {name}"
    slope, intercept = bmap["slope"], bmap["intercept"]
    price_map = load_price_history()

    if not os.path.exists(DB_PATH):
        return f"[rhythm] 数据库不存在: {DB_PATH}"

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT block, from_addr, to_addr, amount FROM transfers WHERE contract=? ORDER BY block ASC",
            (contract.lower(),)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return f"[rhythm] {name}: 无转账记录"

    pair = pair.lower()
    output = []
    output.append(f"\n{'='*60}")
    output.append(f"⏱️ 庄家操盘节奏分析 [{name}]")
    output.append(f"记录: {len(rows)} 条")
    output.append(f"{'='*60}")

    # ═══ 1. 每个庄家的买卖时间线 ═══
    for addr in sorted(whale_set):
        buys, sells, transfers_out, transfers_in = [], [], [], []

        for block, fa, ta, amount in rows:
            if fa == pair and ta == addr:
                ts = block_to_ts(block, slope, intercept)
                price = block_to_price(block, slope, intercept, price_map)
                buys.append({"ts": ts, "amount": amount, "usd": amount * price, "block": block})
            elif fa == addr and ta == pair:
                ts = block_to_ts(block, slope, intercept)
                price = block_to_price(block, slope, intercept, price_map)
                sells.append({"ts": ts, "amount": amount, "usd": amount * price, "block": block})
            elif fa == addr and ta != pair:
                ts = block_to_ts(block, slope, intercept)
                transfers_out.append({"ts": ts, "amount": amount, "to": ta})
            elif ta == addr and fa != pair and fa != "0x0000000000000000000000000000000000000000":
                ts = block_to_ts(block, slope, intercept)
                transfers_in.append({"ts": ts, "amount": amount, "from": fa})

        if not buys and not sells:
            continue

        short = f"{addr[:10]}..{addr[-4:]}"
        output.append(f"\n  📍 {short}")
        output.append(f"    买入: {len(buys)}笔 | 卖出: {len(sells)}笔 | 转入: {len(transfers_in)}笔 | 转出: {len(transfers_out)}笔")

        if buys:
            first_buy = datetime.fromtimestamp(buys[0]["ts"], tz=timezone.utc)
            last_buy = datetime.fromtimestamp(buys[-1]["ts"], tz=timezone.utc)
            total_buy_usd = sum(b["usd"] for b in buys)
            output.append(f"    首买: {first_buy.strftime('%m-%d %H:%M')} | 末买: {last_buy.strftime('%m-%d %H:%M')}")
            output.append(f"    买入总额: ${total_buy_usd:,.0f}")

        if sells:
            first_sell = datetime.fromtimestamp(sells[0]["ts"], tz=timezone.utc)
            last_sell = datetime.fromtimestamp(sells[-1]["ts"], tz=timezone.utc)
            total_sell_usd = sum(s["usd"] for s in sells)
            output.append(f"    首卖: {first_sell.strftime('%m-%d %H:%M')} | 末卖: {last_sell.strftime('%m-%d %H:%M')}")
            output.append(f"    卖出总额: ${total_sell_usd:,.0f}")

        # 活跃时段分析
        all_actions = [(b["ts"], "buy", b["usd"]) for b in buys] + [(s["ts"], "sell", s["usd"]) for s in sells]
        if all_actions:
            hour_stats = defaultdict(lambda: {"buy": 0, "sell": 0, "buy_usd": 0, "sell_usd": 0})
            for ts, action, usd in all_actions:
                h = int(ts % 86400 // 3600)
                hour_stats[h][action] += 1
                hour_stats[h][f"{action}_usd"] += usd

            sorted_hours = sorted(hour_stats.items(), key=lambda x: -(x[1]["buy"] + x[1]["sell"]))
            output.append(f"    活跃时段 (UTC):")
            for h, stats in sorted_hours[:5]:
                output.append(f"      {h:02d}:00 | 买{stats['buy']}笔(${stats['buy_usd']:,.0f}) 卖{stats['sell']}笔(${stats['sell_usd']:,.0f})")

        # 日级别买卖节奏
        if buys or sells:
            day_stats = defaultdict(lambda: {"buy": 0, "sell": 0, "buy_usd": 0, "sell_usd": 0})
            for b in buys:
                day = datetime.fromtimestamp(b["ts"], tz=timezone.utc).strftime("%m-%d")
                day_stats[day]["buy"] += 1
                day_stats[day]["buy_usd"] += b["usd"]
            for s in sells:
                day = datetime.fromtimestamp(s["ts"], tz=timezone.utc).strftime("%m-%d")
                day_stats[day]["sell"] += 1
                day_stats[day]["sell_usd"] += s["usd"]

            pump_days = [(d, s["buy_usd"] - s["sell_usd"], s) for d, s in sorted(day_stats.items()) if s["buy_usd"] - s["sell_usd"] > 1000]
            dump_days = [(d, s["buy_usd"] - s["sell_usd"], s) for d, s in sorted(day_stats.items()) if s["buy_usd"] - s["sell_usd"] < -1000]

            if pump_days:
                output.append(f"    🟢 拉盘日 (净买入>$1k):")
                for day, net, stats in sorted(pump_days, key=lambda x: -x[1])[:5]:
                    output.append(f"      {day} | 净买${net:>+10,.0f} (买{stats['buy']}笔 卖{stats['sell']}笔)")

            if dump_days:
                output.append(f"    🔴 砸盘日 (净卖出>$1k):")
                for day, net, stats in sorted(dump_days, key=lambda x: x[1])[:5]:
                    output.append(f"      {day} | 净卖${net:>+10,.0f} (买{stats['buy']}笔 卖{stats['sell']}笔)")

    # ═══ 2. 全局操盘节奏 ═══
    output.append(f"\n{'='*60}")
    output.append(f"📈 全局操盘节奏（所有庄家合计）[{name}]")
    output.append(f"{'='*60}")

    global_day = defaultdict(lambda: {"buy_usd": 0, "sell_usd": 0, "buy_count": 0, "sell_count": 0})
    for block, fa, ta, amount in rows:
        if fa == pair and ta in whale_set:
            ts = block_to_ts(block, slope, intercept)
            price = block_to_price(block, slope, intercept, price_map)
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d")
            global_day[day]["buy_usd"] += amount * price
            global_day[day]["buy_count"] += 1
        elif fa in whale_set and ta == pair:
            ts = block_to_ts(block, slope, intercept)
            price = block_to_price(block, slope, intercept, price_map)
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d")
            global_day[day]["sell_usd"] += amount * price
            global_day[day]["sell_count"] += 1

    output.append(f"\n  日期      | 买入($)      | 卖出($)      | 净额($)      | 买笔 | 卖笔")
    output.append(f"  {'-'*75}")
    for day in sorted(global_day.keys()):
        d = global_day[day]
        net = d["buy_usd"] - d["sell_usd"]
        icon = "🟢" if net > 0 else "🔴"
        output.append(f"  {day} {icon} | ${d['buy_usd']:>10,.0f} | ${d['sell_usd']:>10,.0f} | ${net:>+10,.0f} | {d['buy_count']:>4} | {d['sell_count']:>4}")

    # ═══ 3. 操盘模式识别 ═══
    output.append(f"\n{'='*60}")
    output.append(f"🔍 操盘模式识别 [{name}]")
    output.append(f"{'='*60}")

    days = sorted(global_day.keys())
    if len(days) >= 3:
        streak_type = None
        streak_count = 0
        max_pump_streak = 0
        max_dump_streak = 0

        for day in days:
            net = global_day[day]["buy_usd"] - global_day[day]["sell_usd"]
            if net > 0:
                if streak_type == "pump":
                    streak_count += 1
                else:
                    streak_type = "pump"
                    streak_count = 1
                max_pump_streak = max(max_pump_streak, streak_count)
            elif net < 0:
                if streak_type == "dump":
                    streak_count += 1
                else:
                    streak_type = "dump"
                    streak_count = 1
                max_dump_streak = max(max_dump_streak, streak_count)

        total_buy = sum(d["buy_usd"] for d in global_day.values())
        total_sell = sum(d["sell_usd"] for d in global_day.values())
        output.append(f"  最长连续拉盘: {max_pump_streak} 天")
        output.append(f"  最长连续砸盘: {max_dump_streak} 天")
        output.append(f"  庄家总买入: ${total_buy:,.0f}")
        output.append(f"  庄家总卖出: ${total_sell:,.0f}")
        if total_sell > 0:
            output.append(f"  买卖比: {total_buy/total_sell:.2f}")

        recent = days[-5:]
        recent_buy = sum(global_day[d]["buy_usd"] for d in recent)
        recent_sell = sum(global_day[d]["sell_usd"] for d in recent)
        output.append(f"\n  最近5天趋势:")
        output.append(f"    买入: ${recent_buy:,.0f} | 卖出: ${recent_sell:,.0f}")
        if recent_sell > recent_buy * 1.5:
            output.append(f"    ⚠️ 近期出货加速")
        elif recent_buy > recent_sell * 1.5:
            output.append(f"    🟢 近期吸筹中")
        else:
            output.append(f"    ➡️ 买卖均衡")

    output.append(f"\n{'='*60}")
    return "\n".join(output)


def main():
    config = load_config()
    contracts = config.get("contracts", {})

    # 如果传了合约地址参数，只分析那一个
    target = None
    if len(sys.argv) > 1:
        target = sys.argv[1].lower()

    for contract, cfg in contracts.items():
        if target and contract.lower() != target:
            continue
        name = cfg.get("name", contract[:10])
        pair = cfg.get("pair", "")
        if not pair:
            print(f"[rhythm] {name}: 缺少 pair 配置，跳过", file=sys.stderr)
            continue
        whale_set = {a.lower() for a in cfg.get("whale_addrs", [])}
        if not whale_set:
            print(f"[rhythm] {name}: 无庄家地址，跳过", file=sys.stderr)
            continue

        result = analyze_rhythm(contract, pair, whale_set, name)
        print(result)

    if target and target not in {c.lower() for c in contracts}:
        print(f"[rhythm] 合约 {target} 不在 config.json 中", file=sys.stderr)


if __name__ == "__main__":
    main()
