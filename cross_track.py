#!/usr/bin/env python3
"""
cross_track.py — 跨合约追踪
查庄家在其他 token 上的操作记录，判断是否惯犯
"""
import os
import sys
import time
import requests
from collections import defaultdict

ALCHEMY_KEY = os.environ.get("ALCHEMY_KEY", "")
def _get_alchemy_url():
    return f"https://bnb-mainnet.g.alchemy.com/v2/{os.environ.get("ALCHEMY_KEY", ALCHEMY_KEY)}"


def get_addr_tokens(addr, direction="from", max_pages=3):
    """
    查询某地址参与过的所有 ERC20 token 转账
    direction: "from" 查卖出/转出, "to" 查买入/转入
    返回: {token_addr: {"amount": float, "count": int, "last_block": int}}
    """
    tokens = defaultdict(lambda: {"amount": 0, "count": 0, "last_block": 0})
    page_key = None

    for page in range(max_pages):
        params = {
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [{
                f"{direction}Address": addr,
                "category": ["erc20"],
                "maxCount": "0x3e8",
                "order": "desc",
                "withMetadata": True,
            }],
            "id": 1
        }
        if page_key:
            params["params"][0]["pageKey"] = page_key

        for retry in range(3):
            try:
                r = requests.post(_get_alchemy_url(), json=params, timeout=30)
                data = r.json()
                break
            except Exception:
                time.sleep(2)
                data = {}

        if "error" in data:
            break

        result = data.get("result") or {}
        transfers = result.get("transfers") or []

        for tx in transfers:
            raw = tx.get("rawContract") or {}
            token_addr = (raw.get("address") or "").lower()
            if not token_addr:
                continue
            value = tx.get("value")
            if value is not None:
                try:
                    amount = float(value)
                except Exception:
                    continue
            else:
                hex_val = raw.get("value") or "0x0"
                try:
                    decimals = int(raw.get("decimal") or raw.get("decimals") or "0x12", 16) if "0x" in str(raw.get("decimal") or raw.get("decimals") or "0x12") else 18
                    amount = int(hex_val, 16) / (10 ** decimals)
                except Exception:
                    continue
            block = int(tx.get("blockNum") or "0x0", 16)
            tokens[token_addr]["amount"] += amount
            tokens[token_addr]["count"] += 1
            tokens[token_addr]["last_block"] = max(tokens[token_addr]["last_block"], block)

        page_key = result.get("pageKey")
        if not page_key or not transfers:
            break
        time.sleep(0.3)

    return dict(tokens)


def get_token_name(addr):
    """快速获取 token 名称"""
    try:
        from scan_core import eth_call, decode_string
        name = decode_string(eth_call(addr, "0x06fdde03"))
        symbol = decode_string(eth_call(addr, "0x95d89b41"))
        return f"{name}({symbol})" if name != "?" else addr[:10]
    except Exception:
        return addr[:10]


def cross_track_whales(whale_addrs, current_contract, max_whales=5, max_pages=2):
    """
    跨合约追踪庄家。
    返回: {
        whale_addr: {
            "other_tokens": [{addr, name, buy_amount, sell_amount, net, verdict}],
            "pattern": str,  # "惯犯" / "新手" / "专注"
            "token_count": int,
        }
    }
    """
    current = current_contract.lower()
    results = {}

    # 常见排除地址（WBNB, USDT, BUSD, PancakeSwap Router 等）
    IGNORE_TOKENS = {
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
        "0x55d398326f99059ff775485246999027b3197955",  # USDT
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
        current,
    }

    for i, addr in enumerate(whale_addrs[:max_whales]):
        print(f"  跨合约追踪 {i+1}/{min(len(whale_addrs), max_whales)}: {addr[:10]}...", file=sys.stderr)

        # 查买入和卖出
        buys = get_addr_tokens(addr, "to", max_pages)
        sells = get_addr_tokens(addr, "from", max_pages)

        # 合并
        all_tokens = set(buys.keys()) | set(sells.keys())
        other_tokens = []

        for token in all_tokens:
            if token in IGNORE_TOKENS or token == current:
                continue
            buy_amt = buys.get(token, {}).get("amount", 0)
            buy_cnt = buys.get(token, {}).get("count", 0)
            sell_amt = sells.get(token, {}).get("amount", 0)
            sell_cnt = sells.get(token, {}).get("count", 0)

            # 只关注有一定交易量的 token
            if buy_cnt + sell_cnt < 3:
                continue

            net = buy_amt - sell_amt
            if sell_amt > buy_amt * 0.8 and sell_cnt > 0:
                verdict = "🔴已出货"
            elif sell_amt == 0 and buy_amt > 0:
                verdict = "📦持有"
            elif buy_amt == 0 and sell_amt > 0:
                verdict = "🔴纯卖"
            else:
                verdict = "🔄交易中"

            other_tokens.append({
                "addr": token,
                "buy_count": buy_cnt,
                "sell_count": sell_cnt,
                "buy_amount": buy_amt,
                "sell_amount": sell_amt,
                "verdict": verdict,
            })

        # 排序：按交易笔数
        other_tokens.sort(key=lambda x: -(x["buy_count"] + x["sell_count"]))

        # 判断模式
        dumped = sum(1 for t in other_tokens if "出货" in t["verdict"] or "纯卖" in t["verdict"])
        total = len(other_tokens)

        if total == 0:
            pattern = "专注"
        elif dumped > total * 0.5 and dumped >= 2:
            pattern = "⚠️惯犯"
        elif dumped >= 1:
            pattern = "有前科"
        else:
            pattern = "正常"

        results[addr] = {
            "other_tokens": other_tokens[:8],
            "pattern": pattern,
            "token_count": total,
            "dumped_count": dumped,
        }

        time.sleep(0.5)

    return results


def format_cross_track(results, tp=0):
    """格式化跨合约追踪报告"""
    lines = []
    lines.append("🔎 跨合约追踪")

    if not results:
        lines.append("  无数据")
        return "\n".join(lines)

    for addr, data in results.items():
        short = f"{addr[:6]}..{addr[-4:]}"
        lines.append(f"\n  {short} [{data['pattern']}] 参与{data['token_count']}个token")

        for t in data["other_tokens"][:5]:
            t_short = f"{t['addr'][:8]}..{t['addr'][-4:]}"
            lines.append(f"    {t_short} 买{t['buy_count']}笔 卖{t['sell_count']}笔 {t['verdict']}")

    # 总结
    criminals = [a for a, d in results.items() if "惯犯" in d["pattern"]]
    if criminals:
        lines.append(f"\n  ⚠️ {len(criminals)}个庄家是惯犯（多个token出货）")

    return "\n".join(lines)
