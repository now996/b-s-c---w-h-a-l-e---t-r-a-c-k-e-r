#!/usr/bin/env python3
"""
snapshot.py — 持仓快照模块
用 Alchemy getTokenBalances 批量查庄家实时持仓
替代遍历全量转账记录推算 balance，速度提升 100x
"""
import os
import sys
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from data_source import get_alchemy_client, get_price_provider, _load_config
from db import get_connection, init_db, save_whale_snapshot, get_latest_snapshot, compare_snapshots


def take_whale_snapshot(contract, whale_addrs, token_price=None):
    """
    用 Alchemy getTokenBalances 批量查庄家实时持仓

    contract: 代币合约地址
    whale_addrs: 庄家地址列表
    token_price: 可选，代币价格（不传则自动获取）

    返回: {
        "contract": str,
        "token_price": float,
        "snapshot_ts": str,
        "whales": [{"addr": str, "balance": float, "usd_value": float}, ...],
        "total_whale_holdings": float,
        "total_whale_usd": float,
    }
    """
    contract = contract.lower()
    client = get_alchemy_client()

    if not client.available:
        print("[snapshot] Alchemy key 不可用，跳过快照", file=sys.stderr)
        return None

    # 获取价格
    if token_price is None:
        token_price, _ = get_price_provider().get_token_price(contract)

    if token_price <= 0:
        print("[snapshot] 无法获取代币价格，跳过快照", file=sys.stderr)
        return None

    # 获取代币精度
    try:
        from data_source import get_rpc_client
        rpc = get_rpc_client()
        dec_hex = rpc.eth_call(contract, "0x313ce567")
        decimals = int(dec_hex, 16) if dec_hex and dec_hex != "0x" else 18
    except Exception:
        decimals = 18

    # 批量查询庄家持仓
    whales = []
    for addr in whale_addrs:
        try:
            balances = client.get_token_balances(addr, [contract])
            raw_balance = balances.get(contract, 0)
            balance = raw_balance / (10 ** decimals) if raw_balance > 0 else 0
            usd_value = balance * token_price

            whales.append({
                "addr": addr.lower(),
                "balance": balance,
                "usd_value": usd_value,
                "raw_balance": raw_balance,
            })
        except Exception as e:
            print(f"[snapshot] 查询 {addr[:10]}.. 失败: {e}", file=sys.stderr)
            whales.append({
                "addr": addr.lower(),
                "balance": 0,
                "usd_value": 0,
                "raw_balance": 0,
            })

        # 控制速率（Alchemy 免费 25 req/s）
        time.sleep(0.05)

    total_holdings = sum(w["balance"] for w in whales)
    total_usd = sum(w["usd_value"] for w in whales)
    snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    result = {
        "contract": contract,
        "token_price": token_price,
        "snapshot_ts": snapshot_ts,
        "whales": whales,
        "total_whale_holdings": total_holdings,
        "total_whale_usd": total_usd,
    }

    # 保存到 DB
    try:
        conn = get_connection(contract)
        init_db(conn)
        whale_balances = {w["addr"]: w["balance"] for w in whales}
        save_whale_snapshot(conn, contract, whale_balances, token_price)
        conn.close()
    except Exception as e:
        print(f"[snapshot] 保存快照失败: {e}", file=sys.stderr)

    return result


def get_snapshot_diff(contract):
    """获取最近两次快照的持仓变化"""
    try:
        conn = get_connection(contract)
        init_db(conn)
        diff = compare_snapshots(conn, contract)
        conn.close()
        return diff
    except Exception as e:
        print(f"[snapshot] 获取快照对比失败: {e}", file=sys.stderr)
        return None


def format_snapshot(snapshot):
    """格式化快照为可读文本"""
    if not snapshot:
        return "无快照数据"

    lines = [
        f"📸 持仓快照 [{snapshot['contract'][:10]}..]",
        f"价格: ${snapshot['token_price']:.8f}",
        f"时间: {snapshot['snapshot_ts']}",
        f"庄家总持仓: {snapshot['total_whale_holdings']:,.0f} (${snapshot['total_whale_usd']:,.0f})",
        "",
    ]

    for w in sorted(snapshot["whales"], key=lambda x: -x["balance"]):
        short = f"{w['addr'][:8]}..{w['addr'][-4:]}"
        lines.append(f"  {short}: {w['balance']:,.0f} (${w['usd_value']:,.0f})")

    return "\n".join(lines)
