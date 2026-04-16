#!/usr/bin/env python3
"""
fund_trace.py — 资金溯源
追踪庄家的 WBNB/资金来源，识别 CEX 提币、其他项目利润等
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

# 已知 CEX 热钱包地址（BSC，小写）
CEX_LABELS = {
    "0x8894e0a0c962cb723c1ef8a1b63d28aaa26e8f6f": "Binance",
    "0xe2fc31f816a9b94326492132018c3aecc4a93ae1": "Binance",
    "0x3c783c21a0383057d128bae431894a5c19f9cf06": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance",
    "0xa180fe01b906a1be37be6c534a3300785b20d947": "KuCoin",
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": "Gate.io",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "OKX",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "OKX",
    "0xeb2d2f1b8c558a40207669291fda468e50c8a0bb": "Coinbase",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Bybit",
    "0xee5b5b923ffce93a870b3104b7ca09c3db80047a": "Bybit",
    "0x8f22f2063d253846b53609231ed80fa571bc0c8f": "MEXC",
    "0x4982085c9e2f89f2ecb8131eca71afad896e89cb": "HTX",
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",
    "0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2": "FTX",
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": "KuCoin",
}

# PancakeSwap 路由器
ROUTER_LABELS = {
    "0x10ed43c718714eb63d5aa57b78b54704e256024e": "PCS V2 Router",
    "0x13f4ea83d0bd40e75c8222255bc855a974568dd4": "PCS V3 Router",
    "0x1b81d678ffb9c0263b24a97847620c99d213eb14": "PCS Universal",
}


def get_bnb_balance(addr):
    try:
        result, _ = rpc_call("eth_getBalance", [addr, "latest"], timeout=10)
        return int(result or "0x0", 16) / 1e18
    except Exception:
        return 0


def get_wbnb_transfers(addr, alchemy_key, direction="to", max_count=20):
    """查 WBNB 转入/转出"""
    url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    params = {
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [{
            f"{direction}Address": addr,
            "contractAddresses": [WBNB],
            "category": ["erc20"],
            "maxCount": hex(max_count),
            "order": "asc",
            "withMetadata": True,
        }],
        "id": 1
    }
    try:
        r = requests.post(url, json=params, timeout=30)
        data = r.json()
        return (data.get("result") or {}).get("transfers") or []
    except Exception:
        return []


def get_internal_bnb_transfers(addr, alchemy_key, direction="to", max_count=20):
    """查 BNB 内部交易（通过合约中转的 BNB）"""
    url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    params = {
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [{
            f"{direction}Address": addr,
            "category": ["internal"],
            "maxCount": hex(max_count),
            "order": "asc",
            "withMetadata": True,
        }],
        "id": 1
    }
    try:
        r = requests.post(url, json=params, timeout=30)
        data = r.json()
        return (data.get("result") or {}).get("transfers") or []
    except Exception:
        return []


def classify_source(addr):
    """分类地址来源"""
    addr = addr.lower()
    if addr in CEX_LABELS:
        return "CEX", CEX_LABELS[addr]
    if addr in ROUTER_LABELS:
        return "DEX", ROUTER_LABELS[addr]
    # 检查是否是合约
    from scan_core import is_contract_address
    if is_contract_address(addr):
        return "合约", None
    return "EOA", None


def trace_whale_funds(whale_addrs, alchemy_key, max_whales=5):
    """追踪庄家资金来源"""
    results = {}

    for i, addr in enumerate(whale_addrs[:max_whales]):
        print(f"  资金溯源 {i+1}/{min(len(whale_addrs), max_whales)}: {addr[:10]}...", file=sys.stderr)

        bnb_bal = get_bnb_balance(addr)

        # 查最早的 WBNB 转入（ERC20）
        wbnb_in = get_wbnb_transfers(addr, alchemy_key, "to", 20)

        # 查 BNB 内部交易（通过合约中转的 BNB）
        internal_in = get_internal_bnb_transfers(addr, alchemy_key, "to", 20)

        sources = []
        source_summary = defaultdict(float)  # type -> total_wbnb

        for tx in wbnb_in:
            fa = (tx.get("from") or "").lower()
            val = float(tx.get("value") or 0)
            ts = (tx.get("metadata") or {}).get("blockTimestamp", "")[:10]

            src_type, src_name = classify_source(fa)
            source_summary[src_type] += val

            sources.append({
                "from": fa,
                "value": val,
                "type": src_type,
                "name": src_name,
                "time": ts,
            })

        # 处理内部交易来源
        for tx in internal_in:
            fa = (tx.get("from") or "").lower()
            val = float(tx.get("value") or 0)
            ts = (tx.get("metadata") or {}).get("blockTimestamp", "")[:10]
            src_type, src_name = classify_source(fa)
            source_summary["internal_" + src_type] = source_summary.get("internal_" + src_type, 0) + val
            sources.append({
                "from": fa,
                "value": val,
                "type": "internal_" + src_type,
                "name": src_name or "内部交易",
                "time": ts,
            })

        # 判断主要来源
        total = sum(source_summary.values())
        if total == 0:
            primary = "未知"
        elif source_summary.get("CEX", 0) > total * 0.3:
            cex_names = set(s["name"] for s in sources if s["type"] == "CEX" and s["name"])
            primary = "CEX (" + "/".join(cex_names) + ")"
        elif source_summary.get("DEX", 0) > total * 0.3:
            primary = "DEX利润"
        elif source_summary.get("合约", 0) > total * 0.5:
            primary = "合约转入"
        else:
            primary = "EOA转入"

        results[addr] = {
            "bnb_balance": bnb_bal,
            "funding_sources": sources[:10],
            "primary_source": primary,
            "total_wbnb_in": total,
            "source_summary": dict(source_summary),
        }

        time.sleep(0.5)

    return results


def format_fund_trace(results):
    """格式化资金溯源报告"""
    lines = []
    lines.append("💸 资金溯源")

    if not results:
        lines.append("  无数据")
        return "\n".join(lines)

    for addr, data in results.items():
        short = f"{addr[:6]}..{addr[-4:]}"
        lines.append(f"\n  {short} 来源: {data['primary_source']}")
        lines.append(f"    BNB余额: {data['bnb_balance']:.3f} | WBNB流入: {data['total_wbnb_in']:.2f}")

        for tx in data["funding_sources"][:3]:
            src_short = f"{tx['from'][:6]}..{tx['from'][-4:]}"
            label = f" [{tx['name'] or tx['type']}]" if tx["type"] != "EOA" else ""
            lines.append(f"    ← {src_short}{label} {tx['value']:.3f} WBNB {tx['time']}")

    # 总结
    cex_count = sum(1 for d in results.values() if "CEX" in d["primary_source"])
    dex_count = sum(1 for d in results.values() if "DEX" in d["primary_source"])
    if cex_count:
        lines.append(f"\n  📊 {cex_count}个庄家资金来自 CEX")
    if dex_count:
        lines.append(f"  📊 {dex_count}个庄家资金来自 DEX 利润")

    return "\n".join(lines)
