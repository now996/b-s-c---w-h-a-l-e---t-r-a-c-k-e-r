#!/usr/bin/env python3
"""
lp_detect.py — LP 操纵检测
识别加池/撤池模式，检测 rug pull 风险
"""
import os
import sys
import time
import json
import requests
from collections import defaultdict

DEFAULT_RPCS = [
    "https://bsc-rpc.publicnode.com",
    "https://bsc.drpc.org",
    "https://rpc.ankr.com/bsc",
    "https://bsc.meowrpc.com",
    "https://1rpc.io/bnb",
    "https://binance.llamarpc.com",
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed1.binance.org",
    "https://bsc-dataseed2.binance.org",
    "https://bsc-dataseed3.binance.org",
    "https://bsc-dataseed4.binance.org"
]
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
ZERO = "0x0000000000000000000000000000000000000000"


def get_rpc_candidates():
    rpcs = []
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    try:
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = json.load(f)
            for item in config.get("bsc_rpcs", []):
                if isinstance(item, str) and item and item not in rpcs:
                    rpcs.append(item)
            primary = config.get("bsc_rpc")
            if isinstance(primary, str) and primary and primary not in rpcs:
                rpcs.append(primary)
    except Exception:
        pass

    env_rpc = os.environ.get("BSC_RPC")
    if env_rpc and env_rpc not in rpcs:
        rpcs.append(env_rpc)

    for item in DEFAULT_RPCS:
        if item not in rpcs:
            rpcs.append(item)
    return rpcs


RPC_CANDIDATES = get_rpc_candidates()
BSC_RPC = RPC_CANDIDATES[0]


def rpc_call(method, params, timeout=10):
    last_error = None
    for rpc in RPC_CANDIDATES:
        try:
            r = requests.post(rpc, json={
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1,
            }, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise RuntimeError(str(data["error"]))
            return data.get("result"), rpc
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"all rpc failed: {last_error}")


def get_lp_events(pair_addr, alchemy_key, max_pages=5):
    """
    获取 LP pair 的 Mint/Burn 事件（加池/撤池）
    通过追踪 LP token 的 mint(from 0x0) 和 burn(to 0x0) 来识别
    """
    url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    events = []
    page_key = None

    for page in range(max_pages):
        params = {
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [{
                "contractAddresses": [pair_addr.lower()],
                "category": ["erc20"],
                "maxCount": "0x3e8",
                "order": "asc",
                "withMetadata": True,
            }],
            "id": 1
        }
        if page_key:
            params["params"][0]["pageKey"] = page_key

        try:
            r = requests.post(url, json=params, timeout=30)
            data = r.json()
        except Exception:
            break

        result = data.get("result") or {}
        transfers = result.get("transfers") or []

        for tx in transfers:
            fa = (tx.get("from") or "").lower()
            ta = (tx.get("to") or "").lower()
            val = float(tx.get("value") or 0)
            block = int(tx.get("blockNum") or "0x0", 16)
            ts = (tx.get("metadata") or {}).get("blockTimestamp", "")

            if fa == ZERO:
                # Mint = 加池
                events.append({
                    "type": "add",
                    "to": ta,
                    "amount": val,
                    "block": block,
                    "time": ts,
                })
            elif ta == ZERO or ta == "0x000000000000000000000000000000000000dead":
                # Burn = 撤池: to 是 zero/dead 地址，from 是执行 burn 的持有者
                events.append({
                    "type": "remove",
                    "from": fa,
                    "to": ta,
                    "amount": val,
                    "block": block,
                    "time": ts,
                })

        page_key = result.get("pageKey")
        if not page_key or not transfers:
            break
        time.sleep(0.3)

    return events


def analyze_lp_manipulation(events, token_records=None, pair_addr=None):
    """
    分析 LP 操纵模式。
    """
    if not events:
        return {
            "total_adds": 0, "total_removes": 0,
            "add_amount": 0, "remove_amount": 0,
            "remove_pct": 0, "patterns": [],
            "lp_providers": {}, "risk_level": "未知",
            "timeline": [],
        }

    total_adds = sum(1 for e in events if e["type"] == "add")
    total_removes = sum(1 for e in events if e["type"] == "remove")
    add_amount = sum(e["amount"] for e in events if e["type"] == "add")
    remove_amount = sum(e["amount"] for e in events if e["type"] == "remove")
    remove_pct = remove_amount / add_amount * 100 if add_amount > 0 else 0

    # 统计每个地址的加池/撤池
    providers = defaultdict(lambda: {"adds": 0, "removes": 0, "add_amount": 0, "remove_amount": 0})
    for e in events:
        if e["type"] == "add":
            addr = e.get("to", "?")
            providers[addr]["adds"] += 1
            providers[addr]["add_amount"] += e["amount"]
        else:
            addr = e.get("from", "?")
            providers[addr]["removes"] += 1
            providers[addr]["remove_amount"] += e["amount"]

    # 检测模式
    patterns = []

    # 1. 单一 LP 提供者（高度集中）
    if len(providers) == 1:
        patterns.append("⚠️ 单一LP提供者（随时可撤池跑路）")
    elif len(providers) <= 3:
        patterns.append("⚠️ LP高度集中（仅" + str(len(providers)) + "个提供者）")

    # 2. 大量撤池
    if remove_pct > 80:
        patterns.append("🔴 已撤池 " + f"{remove_pct:.0f}%（几乎跑路）")
    elif remove_pct > 50:
        patterns.append("🟠 撤池过半 " + f"{remove_pct:.0f}%")
    elif remove_pct > 20:
        patterns.append("🟡 部分撤池 " + f"{remove_pct:.0f}%")

    # 3. 快速加池后撤池（rug pull 经典模式）
    if events:
        first_add = next((e for e in events if e["type"] == "add"), None)
        first_remove = next((e for e in events if e["type"] == "remove"), None)
        if first_add and first_remove:
            block_diff = first_remove["block"] - first_add["block"]
            if block_diff < 1000:
                patterns.append("🔴 加池后快速撤池（间隔<1000区块）")
            elif block_diff < 10000:
                patterns.append("🟠 加池后较快撤池")

    # 4. 反复加撤（做市操纵）
    if total_adds > 5 and total_removes > 5:
        patterns.append("⚠️ 反复加撤池（" + f"{total_adds}次加/{total_removes}次撤）")

    # 5. LP 锁定检测（burn 事件的 to 是 zero/dead 地址）
    dead_burns = sum(1 for e in events if e["type"] == "remove" and e.get("to", "").lower() in {ZERO, "0x000000000000000000000000000000000000dead"})
    if dead_burns > 0:
        patterns.append("✅ 部分LP已销毁/锁定")

    # 风险等级
    if remove_pct > 80 or "快速撤池" in str(patterns):
        risk_level = "🔴极高"
    elif remove_pct > 50 or len(providers) == 1:
        risk_level = "🟠高"
    elif remove_pct > 20 or len(providers) <= 3:
        risk_level = "🟡中"
    else:
        risk_level = "🟢低"

    # 时间线（最近的事件）
    timeline = []
    for e in events[-10:]:
        addr = e.get("to") or e.get("from", "?")
        timeline.append({
            "type": "加池" if e["type"] == "add" else "撤池",
            "addr": addr,
            "amount": e["amount"],
            "time": e.get("time", "")[:16],
        })

    return {
        "total_adds": total_adds,
        "total_removes": total_removes,
        "add_amount": add_amount,
        "remove_amount": remove_amount,
        "remove_pct": remove_pct,
        "patterns": patterns,
        "lp_providers": dict(providers),
        "risk_level": risk_level,
        "timeline": timeline,
        "provider_count": len(providers),
    }


def format_lp_report(lp_data):
    """格式化 LP 操纵报告"""
    lines = []
    lines.append(f"🏊 LP 分析 [{lp_data['risk_level']}]")

    lines.append(f"  加池 {lp_data['total_adds']}次 | 撤池 {lp_data['total_removes']}次")
    if lp_data['add_amount'] > 0:
        lines.append(f"  撤池比例: {lp_data['remove_pct']:.1f}%")
    lines.append(f"  LP提供者: {lp_data['provider_count']}个")

    if lp_data["patterns"]:
        for p in lp_data["patterns"]:
            lines.append(f"  {p}")

    if lp_data["timeline"]:
        lines.append("  最近:")
        for t in lp_data["timeline"][-5:]:
            short = f"{t['addr'][:6]}..{t['addr'][-4:]}"
            lines.append(f"    {t['type']} {short} {t['amount']:.4f} LP {t['time']}")

    return "\n".join(lines)
