#!/usr/bin/env python3
"""
notify.py — 统一通知模块
- Telegram 通知（修复 403 无限重试问题）
- Webhook 通知（钉钉/飞书/企业微信）
- 消息自动分段（Telegram 4096 字符限制）
"""
import requests
import time
import sys
import html
import json
import os


# ═══════════════════════════════════════════
# Telegram
# ═══════════════════════════════════════════

# Telegram 状态追踪
_telegram_fail_count = 0
_telegram_last_fail_time = 0
_telegram_permanently_disabled = False  # 403 (Bot被拉黑) 后禁用
_telegram_disabled_since = 0             # 禁用时间戳
_TELEGRAM_RETRY_INTERVAL = 3600          # 每小时重试一次（用户可能重新启用 Bot）
_TELEGRAM_MAX_CONTINUOUS_FAILS = 10     # 连续失败 10 次后进入静默模式
_TELEGRAM_SILENT_DURATION = 300         # 静默模式持续 5 分钟


def send_telegram(token, chat_id, message, retries=3):
    """发送 Telegram 消息，修复 403 无限重试问题"""
    global _telegram_fail_count, _telegram_last_fail_time, _telegram_permanently_disabled

    # 永久禁用检查：403 后不再尝试，但每小时重试一次（用户可能重新启用 Bot）
    if _telegram_permanently_disabled:
        if _telegram_disabled_since and time.time() - _telegram_disabled_since > _TELEGRAM_RETRY_INTERVAL:
            _telegram_permanently_disabled = False  # 重置，允许重试
        else:
            return False

    # 静默模式检查：连续失败过多时，暂停发送
    if _telegram_fail_count >= _TELEGRAM_MAX_CONTINUOUS_FAILS:
        if time.time() - _telegram_last_fail_time < _TELEGRAM_SILENT_DURATION:
            # 静默期内，不发送也不重试
            return False
        else:
            # 静默期结束，重置计数器重试
            _telegram_fail_count = 0

    # 消息过长时自动分段
    messages = _split_message(message, max_len=4000)

    all_ok = True
    for msg in messages:
        if not _send_telegram_single(token, chat_id, msg, retries):
            all_ok = False

    if all_ok:
        _telegram_fail_count = 0  # 成功则重置
    else:
        _telegram_fail_count += 1
        _telegram_last_fail_time = time.time()

    return all_ok


def _send_telegram_single(token, chat_id, message, retries=3):
    """发送单条 Telegram 消息"""
    global _telegram_permanently_disabled
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    attempt = 0
    while attempt < retries:
        try:
            r = requests.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=10)
            if r.status_code == 200:
                return True

            # 403 = Bot 被拉黑，永久禁用！只输出一次日志
            if r.status_code == 403:
                if not _telegram_permanently_disabled:
                    _telegram_permanently_disabled = True
                    _telegram_disabled_since = time.time()
                    print(f"[notify] Telegram 403: Bot 被拉黑或无权限，永久禁用 Telegram 通道", file=sys.stderr)
                return False

            # 429 = 限速，等待后重试
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 5)
                time.sleep(wait)
                continue  # 429 不计入重试次数

            print(f"[notify] Telegram {r.status_code}: {r.text[:200]}", file=sys.stderr)
        except Exception as e:
            print(f"[notify] Telegram error: {e}", file=sys.stderr)
            time.sleep(2)
        attempt += 1
    return False


def _split_message(message, max_len=4000):
    """将过长消息分段发送（Telegram 限制 4096 字符）"""
    if len(message) <= max_len:
        return [message]

    parts = []
    while message:
        if len(message) <= max_len:
            parts.append(message)
            break

        # 尝试在换行处分割
        split_pos = message.rfind("\n", 0, max_len)
        if split_pos <= max_len // 2:
            split_pos = max_len

        parts.append(message[:split_pos])
        message = message[split_pos:].lstrip("\n")

    return parts


def _safe(text):
    """转义 HTML 特殊字符"""
    if not isinstance(text, str):
        text = str(text)
    return html.escape(text)


# ═══════════════════════════════════════════
# Webhook 通知（钉钉/飞书/企业微信）
# ═══════════════════════════════════════════

def send_webhook(url, message, webhook_type="dingtalk", retries=2):
    """发送 Webhook 通知"""
    if not url:
        return False

    for attempt in range(retries):
        try:
            # 转为纯文本用于 Webhook（去掉 HTML 标签）
            plain_msg = _strip_html_tags(message) if "<" in message else message

            if webhook_type == "dingtalk":
                # 钉钉机器人 Webhook（Markdown 格式）
                payload = {
                    "msgtype": "markdown",
                    "markdown": {"title": "BSC鲸鱼追踪", "text": plain_msg}
                }
            elif webhook_type == "feishu":
                # 飞书机器人 Webhook（富文本格式）
                payload = {
                    "msg_type": "interactive",
                    "card": {
                        "elements": [{"tag": "markdown", "content": plain_msg}],
                        "header": {"title": {"tag": "plain_text", "content": "BSC鲸鱼追踪"}}
                    }
                }
            elif webhook_type == "wecom":
                # 企业微信 Webhook（Markdown 格式）
                payload = {
                    "msgtype": "markdown",
                    "markdown": {"content": plain_msg}
                }
            elif webhook_type == "custom":
                # 自定义 Webhook（JSON POST）
                payload = {"message": plain_msg}
            else:
                payload = {"message": plain_msg}

            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                return True
            print(f"[notify] Webhook {webhook_type} {r.status_code}: {r.text[:100]}", file=sys.stderr)
        except Exception as e:
            print(f"[notify] Webhook error: {e}", file=sys.stderr)
            time.sleep(1)

    return False


# ═══════════════════════════════════════════
# 统一通知函数
# ═══════════════════════════════════════════

def create_notifier(config):
    """创建统一通知函数，支持 Telegram + Webhook 多通道"""
    token = config.get("telegram_bot_token", "")
    chat_id = config.get("telegram_chat_id", "")
    notify_cfg = config.get("notify") or {}
    webhook_url = notify_cfg.get("webhook_url", "")
    webhook_type = notify_cfg.get("webhook_type", "dingtalk")

    def notify(msg):
        results = []

        # 通道1: Telegram
        if token and chat_id:
            ok = send_telegram(token, chat_id, msg)
            results.append(("telegram", ok))

        # 通道2: Webhook（钉钉/飞书/企业微信）
        if webhook_url:
            # Webhook 发纯文本（去掉 HTML 标签）
            plain_msg = _strip_html_tags(msg)
            ok = send_webhook(webhook_url, plain_msg, webhook_type)
            results.append(("webhook", ok))

        # 如果所有通道都不可用，打印到 stderr
        if not results:
            print(f"[notify] {msg[:200]}", file=sys.stderr)

        return any(ok for _, ok in results)

    return notify


def _strip_html_tags(text):
    """简单去除 HTML 标签（用于 Webhook 纯文本发送）"""
    import re
    text = re.sub(r'<[^>]+>', '', text)
    # 解码 HTML 实体
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&#39;', "'").replace('&quot;', '"')
    return text


# ═══════════════════════════════════════════
# 格式化函数（保持向后兼容）
# ═══════════════════════════════════════════

def format_whale_alert(name, addr, action, amount, usd, price):
    short = f"{addr[:8]}..{addr[-4:]}"
    icon = "🟢买入" if action == "buy" else "🔴卖出" if action == "sell" else "↔️转账"
    return (
        f"🐋 <b>庄家动作 [{_safe(name)}]</b>\n"
        f"{icon} {short}\n"
        f"数量: {amount:,.0f}\n"
        f"价值: ${usd:,.0f}\n"
        f"价格: ${price:.8f}"
    )


def format_large_trade(name, action, amount, usd, price, addr):
    short = f"{addr[:8]}..{addr[-4:]}"
    icon = "🟢" if action == "buy" else "🔴"
    return (
        f"{icon} <b>大额交易 [{_safe(name)}]</b>\n"
        f"{'买入' if action == 'buy' else '卖出'} {short}\n"
        f"数量: {amount:,.0f} | ${usd:,.0f}\n"
        f"价格: ${price:.8f}"
    )


def format_lp_change(name, old_bnb, new_bnb, old_token, new_token, bnb_price):
    bnb_diff = new_bnb - old_bnb
    token_diff = new_token - old_token
    usd_diff = bnb_diff * bnb_price
    icon = "🟩加池" if bnb_diff > 0 else "🟥撤池"
    return (
        f"💧 <b>LP变化 [{_safe(name)}]</b>\n"
        f"{icon}\n"
        f"BNB: {old_bnb:.2f} → {new_bnb:.2f} ({bnb_diff:+.2f})\n"
        f"Token: {old_token:,.0f} → {new_token:,.0f} ({token_diff:+,.0f})\n"
        f"价值变化: ${usd_diff:+,.0f}"
    )


def format_daily_report(name, data):
    lines = [f"📊 <b>日报 [{_safe(name)}]</b>", ""]
    if "price" in data:
        lines.append(f"价格: ${data['price']:.8f}")
    if "price_change" in data:
        lines.append(f"24h涨跌: {data['price_change']}%")
    if "volume_24h" in data:
        lines.append(f"24h量: ${data['volume_24h']:,.0f}")
    if "liquidity" in data:
        lines.append(f"流动性: ${data['liquidity']:,.0f}")
    lines.append("")

    if "whale_summary" in data:
        lines.append("<b>庄家动态:</b>")
        for w in data["whale_summary"]:
            addr = w.get("addr", "?")
            short = f"{addr[:8]}..{addr[-4:]}" if len(addr) >= 12 else addr
            lines.append(f"  {short} 持{w.get('balance', 0):,.0f} 买{w.get('buys', 0)}笔 卖{w.get('sells', 0)}笔")
        lines.append("")

    if "cost_analysis" in data:
        lines.append("<b>庄家成本:</b>")
        for c in data["cost_analysis"]:
            addr = c.get("addr", "?")
            short = f"{addr[:8]}..{addr[-4:]}" if len(addr) >= 12 else addr
            pnl_icon = "📈" if c.get("pnl_pct", 0) > 0 else "📉"
            lines.append(f"  {short} 成本${c.get('avg_cost', 0):.6f} {pnl_icon}{c.get('pnl_pct', 0):+.1f}%")
        lines.append("")

    if "sell_pressure" in data:
        sp = data["sell_pressure"]
        lines.append(f"<b>抛压预估:</b>")
        lines.append(f"  庄家剩余: {sp['remaining']:,.0f} (${sp['remaining_usd']:,.0f})")
        lines.append(f"  全抛价格影响: -{sp['price_impact_pct']:.1f}%")
        lines.append("")

    if "retail_stats" in data:
        rs = data["retail_stats"]
        lines.append(f"<b>散户:</b>")
        lines.append(f"  总数: {rs['count']} | 平均持仓: {rs['avg_hold']:,.0f}")
        lines.append(f"  被套比例: {rs['underwater_pct']:.1f}%")

    return "\n".join(lines)
