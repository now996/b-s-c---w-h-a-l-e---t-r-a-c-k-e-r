#!/usr/bin/env python3
"""
new_token_scanner.py — 新币自动扫描
监控 PancakeSwap 新上币，自动跑快速分析
"""
import os
import sys
import time
import json
import requests
from datetime import datetime

DEFAULT_RPCS = [
    "https://bsc-rpc.publicnode.com",
    "https://bsc.drpc.org",
    "https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",
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
ALCHEMY_KEY = os.environ.get("ALCHEMY_KEY", "")
def _get_alchemy_url():
    return f"https://bnb-mainnet.g.alchemy.com/v2/{os.environ.get("ALCHEMY_KEY", ALCHEMY_KEY)}"


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

# PairCreated event topic (Uniswap V2 标准，所有 V2 fork 共用)
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# 多 DEX V2 Factory 列表
V2_FACTORIES = [
    ("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73", "PancakeSwap"),
    ("0x858E3312ed3A876947EA49d572A7C42DE08af7EE", "Biswap"),
    ("0x86407bEa2078ea5f5EB5A52B2caA963bC1F889Da", "BabySwap"),
    ("0x3CD1C46068dAEa5Ebb0d3f55F6915B10648062B8", "MDEX"),
]

WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower()
USDT = "0x55d398326f99059fF775485246999027B3197955".lower()
BUSD = "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56".lower()
STABLES = {WBNB, USDT, BUSD}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, "data", "scanner_state.json")
RESULTS_FILE = os.path.join(SCRIPT_DIR, "data", "new_tokens.jsonl")


def get_latest_block():
    try:
        result, _ = rpc_call("eth_blockNumber", [], timeout=10)
        return int(result, 16)
    except Exception:
        return 0


def get_new_pairs(from_block, to_block):
    """查询多个 DEX V2 Factory 新建的交易对"""
    pairs = []
    seen_pairs = set()
    CHUNK = 2000
    for factory_addr, dex_name in V2_FACTORIES:
        for start in range(from_block, to_block + 1, CHUNK):
            end = min(start + CHUNK - 1, to_block)
            try:
                logs = None
                rpcs_to_try = [_get_alchemy_url()] + RPC_CANDIDATES[:5]
                for rpc_url in rpcs_to_try:
                    try:
                        r = requests.post(rpc_url, json={
                            "jsonrpc": "2.0",
                            "method": "eth_getLogs",
                            "params": [{
                                "fromBlock": hex(start),
                                "toBlock": hex(end),
                                "address": factory_addr,
                                "topics": [PAIR_CREATED_TOPIC],
                            }],
                            "id": 1
                        }, timeout=30)
                        r.raise_for_status()
                        result = r.json().get("result", [])
                        if isinstance(result, list):
                            logs = result
                            break
                    except Exception:
                        continue
                if logs is None:
                    continue
                for log in logs:
                    topics = log.get("topics", [])
                    data = log.get("data", "0x")
                    block = int(log.get("blockNumber", "0x0"), 16)
                    if len(topics) >= 3:
                        token0 = "0x" + topics[1][-40:]
                        token1 = "0x" + topics[2][-40:]
                        pair_addr = "0x" + data[26:66] if len(data) >= 66 else "?"
                        if token0.lower() in STABLES:
                            new_token = token1
                            base_token = token0
                        elif token1.lower() in STABLES:
                            new_token = token0
                            base_token = token1
                        else:
                            continue
                        # 同一 token 在多个 DEX 只记录一次（去重）
                        dedup_key = new_token.lower()
                        if dedup_key in seen_pairs:
                            continue
                        seen_pairs.add(dedup_key)
                        pairs.append({
                            "token": new_token.lower(),
                            "base": base_token.lower(),
                            "pair": pair_addr.lower(),
                            "dex": dex_name,
                            "block": block,
                        })
            except Exception as e:
                print(f"[scanner] getLogs error {dex_name} chunk {start}-{end}: {e}", file=sys.stderr)
            time.sleep(0.2)
    return pairs


def quick_check(token_addr):
    """快速检查新 token 的基本信息，判断是否值得深入分析"""
    from scan_core import get_token_info, get_token_price

    info = get_token_info(token_addr)
    tp, dex_data = get_token_price(token_addr)

    if not info.get("pair"):
        return None

    # 基本过滤
    liq = 0
    if dex_data:
        liq = float(dex_data.get("liquidity", {}).get("usd", 0))

    # 过滤掉流动性太低的（< $1000）
    if liq < 1000:
        return None

    return {
        "name": info["name"],
        "symbol": info["symbol"],
        "total_supply": info["total_supply"],
        "owner": info["owner"],
        "pair": info["pair"],
        "price": tp,
        "liquidity": liq,
        "fdv": float(dex_data.get("fdv") or 0) if dex_data else 0,
    }


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"last_block": 0, "scanned_tokens": []}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def append_result(result):
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")


def scan_once(lookback_blocks=7200):
    """
    扫描一次新币。
    lookback_blocks: 回看多少个区块（默认7200 ≈ 1小时）
    返回: [{token, name, symbol, liquidity, fdv, ...}]
    """
    state = load_state()
    latest = get_latest_block()

    if state["last_block"] == 0:
        from_block = latest - lookback_blocks
    else:
        from_block = state["last_block"] + 1

    to_block = latest
    if to_block <= from_block:
        return []

    print(f"[scanner] 扫描区块 {from_block} -> {to_block} ({to_block - from_block} blocks)", file=sys.stderr)

    pairs = get_new_pairs(from_block, to_block)
    print(f"[scanner] 发现 {len(pairs)} 个新交易对", file=sys.stderr)

    scanned = set(state.get("scanned_tokens", []))
    results = []

    for p in pairs:
        token = p["token"]
        if token in scanned:
            continue

        print(f"[scanner] 检查 {token[:10]}...", file=sys.stderr)
        check = quick_check(token)

        if check:
            result = {
                "token": token,
                "pair": p["pair"],
                "base": p["base"],
                "block": p["block"],
                "time": datetime.utcnow().isoformat(),
                **check,
            }
            results.append(result)
            append_result(result)
            print(f"[scanner] ✅ {check['name']}({check['symbol']}) 池子${check['liquidity']:,.0f} FDV${check['fdv']:,.0f}", file=sys.stderr)

        scanned.add(token)
        time.sleep(0.5)

    # 保存状态（只保留最近1000个已扫描token）
    state["last_block"] = to_block
    state["scanned_tokens"] = list(scanned)[-1000:]
    save_state(state)

    return results


def format_new_tokens(results):
    """格式化新币扫描结果"""
    if not results:
        return "🔍 新币扫描：无新发现"

    lines = [f"🆕 发现 {len(results)} 个新币"]
    for r in results[:10]:
        owner_tag = "✅" if r["owner"] == "0x" + "0" * 40 else "⚠️"
        lines.append(f"\n  {r['name']}({r['symbol']}) {owner_tag}")
        lines.append(f"    合约: {r['token'][:10]}..{r['token'][-4:]}")
        lines.append(f"    池子: ${r['liquidity']:,.0f} | FDV: ${r['fdv']:,.0f}")
        lines.append(f"    价格: ${r['price']:.8f}")
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=7200, help="回看区块数")
    parser.add_argument("--loop", action="store_true", help="持续扫描模式")
    parser.add_argument("--interval", type=int, default=300, help="扫描间隔(秒)")
    args = parser.parse_args()

    if args.loop:
        print("[scanner] 持续扫描模式启动", file=sys.stderr)
        while True:
            try:
                results = scan_once(args.lookback)
                if results:
                    print(format_new_tokens(results))
                else:
                    print(f"[scanner] {datetime.utcnow().isoformat()} 无新币", file=sys.stderr)
            except Exception as e:
                print(f"[scanner] 错误: {e}", file=sys.stderr)
            time.sleep(args.interval)
    else:
        results = scan_once(args.lookback)
        print(format_new_tokens(results))
