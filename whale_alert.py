"""
whale_alert.py — 跟庄预警策略
检测庄家行为模式变化，生成买入/卖出信号
"""
import json
import os
import time
from collections import defaultdict

STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
ALERT_STATE_FILE = os.path.join(STATE_DIR, "alert_state.json")

# 时间窗口（秒）
WINDOW_SHORT = 300    # 5分钟
WINDOW_MEDIUM = 1800  # 30分钟
WINDOW_LONG = 3600    # 1小时


def load_state():
    if os.path.exists(ALERT_STATE_FILE):
        try:
            with open(ALERT_STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[whale_alert] 状态文件损坏，重置: {e}", file=__import__('sys').stderr)
    return {}


def save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp_path = ALERT_STATE_FILE + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, ALERT_STATE_FILE)
    except Exception as e:
        print(f"[whale_alert] 保存状态失败: {e}", file=__import__('sys').stderr)


def analyze_whale_pattern(events, whale_set, pair, token_price):
    """
    分析最近事件，检测庄家行为模式
    events: [{time, from, to, amount, usd, type}]
    返回: [alert_message, ...]
    """
    now = time.time()
    alerts = []
    pair = pair.lower()

    # 按庄家分组统计最近活动（整个1小时窗口，用于信号1-3和6）
    whale_activity = defaultdict(lambda: {
        "buy_count": 0, "sell_count": 0, "transfer_out_count": 0,
        "buy_usd": 0, "sell_usd": 0, "transfer_out_amount": 0,
        "buy_amount": 0, "sell_amount": 0,
    })

    # 分窗口统计：前30分钟 vs 后30分钟（用于加速信号4-5）
    half_window = WINDOW_LONG / 2  # 30分钟
    prev_half_buy = 0   # 30-60分钟前
    prev_half_sell = 0
    curr_half_buy = 0   # 最近30分钟
    curr_half_sell = 0

    for e in events:
        age = now - e.get("time", now)
        if age > WINDOW_LONG:
            continue

        from_addr = e.get("from", "").lower()
        to_addr = e.get("to", "").lower()
        amount = e.get("amount", 0)
        usd = e.get("usd") or 0

        # 庄家从LP买入
        if from_addr == pair and to_addr in whale_set:
            whale_activity[to_addr]["buy_count"] += 1
            whale_activity[to_addr]["buy_usd"] += usd
            whale_activity[to_addr]["buy_amount"] += amount
            # 分窗口
            if age <= half_window:
                curr_half_buy += usd
            else:
                prev_half_buy += usd

        # 庄家卖到LP
        elif to_addr == pair and from_addr in whale_set:
            whale_activity[from_addr]["sell_count"] += 1
            whale_activity[from_addr]["sell_usd"] += usd
            whale_activity[from_addr]["sell_amount"] += amount
            if age <= half_window:
                curr_half_sell += usd
            else:
                prev_half_sell += usd

        # 庄家转出（非LP）
        elif from_addr in whale_set and to_addr != pair:
            whale_activity[from_addr]["transfer_out_count"] += 1
            whale_activity[from_addr]["transfer_out_amount"] += amount

    # ═══ 信号检测 ═══

    total_whale_buy = sum(a["buy_usd"] for a in whale_activity.values())
    total_whale_sell = sum(a["sell_usd"] for a in whale_activity.values())
    total_whale_transfer_out = sum(a["transfer_out_amount"] for a in whale_activity.values())

    # 1. 庄家密集买入信号
    if total_whale_buy > 5000:
        if total_whale_sell == 0 or total_whale_buy > total_whale_sell * 3:
            top_buyer = max(whale_activity.items(), key=lambda x: x[1]["buy_usd"])
            short = f"{top_buyer[0][:8]}..{top_buyer[0][-4:]}"
            ratio_text = f"买/卖比 {total_whale_buy/max(total_whale_sell,1):.1f}x" if total_whale_sell > 0 else "无卖出"
            alerts.append({
                "type": "accumulation",
                "level": "🟢",
                "title": "庄家吸筹信号",
                "detail": f"1h内庄家净买入${total_whale_buy:,.0f}\n"
                          f"{ratio_text}\n"
                          f"主力: {short} 买${top_buyer[1]['buy_usd']:,.0f}",
            })

    # 2. 庄家密集卖出信号
    if total_whale_sell > 5000:
        if total_whale_buy == 0 or total_whale_sell > total_whale_buy * 3:
            top_seller = max(whale_activity.items(), key=lambda x: x[1]["sell_usd"])
            short = f"{top_seller[0][:8]}..{top_seller[0][-4:]}"
            ratio_text = f"卖/买比 {total_whale_sell/max(total_whale_buy,1):.1f}x" if total_whale_buy > 0 else "无买入"
            alerts.append({
                "type": "distribution",
                "level": "🔴",
                "title": "庄家出货信号",
                "detail": f"1h内庄家净卖出${total_whale_sell:,.0f}\n"
                          f"{ratio_text}\n"
                          f"主力: {short} 卖${top_seller[1]['sell_usd']:,.0f}",
            })

    # 3. 庄家大量转出（准备砸盘）
    if total_whale_transfer_out > 1000000 * token_price:
        alerts.append({
            "type": "transfer_warning",
            "level": "⚠️",
            "title": "庄家大量转出",
            "detail": f"1h内转出 {total_whale_transfer_out:,.0f} 枚\n"
                      f"价值 ${total_whale_transfer_out * token_price:,.0f}\n"
                      f"可能准备通过新地址砸盘",
        })

    # 4. 买入突然加速（最近30分钟 vs 前30分钟，非重叠窗口）
    if curr_half_buy > prev_half_buy * 3 and curr_half_buy > 2000 and prev_half_buy > 0:
        alerts.append({
            "type": "buy_acceleration",
            "level": "🟡",
            "title": "庄家买入加速",
            "detail": f"近30min买入${curr_half_buy:,.0f}\n"
                      f"前30min${prev_half_buy:,.0f}\n"
                      f"加速 {curr_half_buy/prev_half_buy:.1f}x",
        })

    # 5. 卖出突然加速（最近30分钟 vs 前30分钟）
    if curr_half_sell > prev_half_sell * 3 and curr_half_sell > 2000 and prev_half_sell > 0:
        alerts.append({
            "type": "sell_acceleration",
            "level": "🔴",
            "title": "庄家卖出加速",
            "detail": f"近30min卖出${curr_half_sell:,.0f}\n"
                      f"前30min${prev_half_sell:,.0f}\n"
                      f"加速 {curr_half_sell/prev_half_sell:.1f}x",
        })

    # 6. 多个庄家同时行动
    active_buyers = sum(1 for a in whale_activity.values() if a["buy_count"] > 3)
    active_sellers = sum(1 for a in whale_activity.values() if a["sell_count"] > 3)

    if active_buyers >= 3:
        alerts.append({
            "type": "coordinated_buy",
            "level": "🟢🟢",
            "title": "多庄协同买入",
            "detail": f"{active_buyers}个庄家同时买入\n"
                      f"总买入${total_whale_buy:,.0f}",
        })

    if active_sellers >= 3:
        alerts.append({
            "type": "coordinated_sell",
            "level": "🔴🔴",
            "title": "多庄协同卖出",
            "detail": f"{active_sellers}个庄家同时卖出\n"
                      f"总卖出${total_whale_sell:,.0f}\n"
                      f"⚠️ 高风险，考虑离场",
        })

    # 更新状态（不再用于加速检测，保留用于其他可能的用途）
    state = load_state()
    state["last_hour_buy_usd"] = total_whale_buy
    state["last_hour_sell_usd"] = total_whale_sell
    state["last_check"] = now
    save_state(state)

    return alerts


def format_alert(alert, token_name="Token"):
    """格式化预警消息"""
    return (
        f"{alert['level']} {alert['title']} [{token_name}]\n"
        f"{alert['detail']}"
    )
