"""
monitor.py — 实时监控模块
轮询 PancakeSwap Swap 事件、庄家 Transfer、LP 变化
并支持分仓停放地址激活监控（gas / nonce / 转出 / 卖进LP）
"""
import sys
import time
import json
import os
import sqlite3

from datetime import datetime, timedelta

# 统一数据源层 — 消除重复代码
from data_source import RPCClient, PriceProvider, get_rpc_client, get_price_provider

SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
WATCHED_STATE_FILE = os.path.join(LOG_DIR, "watched_state.json")
MONITOR_STATE_FILE = os.path.join(LOG_DIR, "monitor_state.json")
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


def short_addr(addr):
    return f"{addr[:8]}..{addr[-4:]}" if addr and len(addr) >= 12 else addr


def default_watch_state(now=None):
    return {
        "bnb_balance": 0,
        "nonce": 0,
        "gas_alerted": False,
        "nonce_alerted": False,
        "stage": "静止",
        "last_action": "",
        "last_update": now or time.time(),
    }


def normalize_watch_state(state, now=None):
    merged = default_watch_state(now)
    if isinstance(state, dict):
        merged.update(state)
    return merged


def load_json_file(path, default):
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception as e:
        print(f"[monitor] 读取 {path} 失败: {e}", file=sys.stderr)
    return default


def save_json_file(path, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 原子写入：先写临时文件再 rename
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception as e:
        print(f"[monitor] 写入 {path} 失败: {e}", file=sys.stderr)


def load_watched_state():
    raw = load_json_file(WATCHED_STATE_FILE, {})
    now = time.time()
    if not isinstance(raw, dict):
        return {}
    return {k.lower(): normalize_watch_state(v, now) for k, v in raw.items()}


def save_watched_state(state):
    normalized = {k.lower(): normalize_watch_state(v) for k, v in state.items()} if isinstance(state, dict) else {}
    save_json_file(WATCHED_STATE_FILE, normalized)


def load_monitor_state():
    return load_json_file(MONITOR_STATE_FILE, {})


def save_monitor_state(state):
    save_json_file(MONITOR_STATE_FILE, state)


def load_runtime_config(config_path):
    try:
        with open(config_path) as f:
            return json.load(f)
    except Exception:
        return None


def apply_config_reload(runtime_config, watched_state, lp_state, rpcs):
    now = time.time()
    contracts = runtime_config.get("contracts", {})
    for contract, cfg in contracts.items():
        pairs_list = cfg.get("pairs", [])
        single_pair = (cfg.get("pair") or "").lower()
        if not pairs_list and single_pair:
            pairs_list = [{"pair": single_pair, "dex": "PancakeSwap-v2"}]
        td = get_token_decimals(contract, rpcs)
        for pi in pairs_list:
            p = (pi["pair"] if isinstance(pi, dict) else pi).lower()
            dex = pi.get("dex", "") if isinstance(pi, dict) else ""
            if p and p not in lp_state:
                reserves = get_reserves(p, rpcs, td)
                lp_state[p] = reserves
                print(f"[monitor] 热加载合约: {cfg.get('name', contract)}[{dex}] | LP {reserves[0]:.2f} BNB / {reserves[1]:,.0f} Token (decimals={td})", file=sys.stderr)

        for addr in cfg.get("watched_wallets", []):
            addr = addr.lower()
            if addr not in watched_state:
                watched_state[addr] = normalize_watch_state(None, now)
                watched_state[addr]["bnb_balance"] = get_native_balance(rpcs, addr)
                watched_state[addr]["nonce"] = get_nonce(rpcs, addr)
                print(f"[monitor] 热加载 watched 地址: {addr}", file=sys.stderr)

    save_watched_state(watched_state)
    return contracts


def get_rpc_candidates(config):
    rpcs = []
    for item in config.get("bsc_rpcs", []):
        if isinstance(item, str) and item and item not in rpcs:
            rpcs.append(item)

    primary = config.get("bsc_rpc")
    if isinstance(primary, str) and primary and primary not in rpcs:
        rpcs.append(primary)

    for item in DEFAULT_RPCS:
        if item not in rpcs:
            rpcs.append(item)

    return rpcs


def rpc_call(rpcs, method, params, timeout=10):
    """统一 RPC 调用 — 复用 data_source.RPCClient，保持向后兼容签名"""
    client = get_rpc_client()
    if rpcs is not None and rpcs != client.rpcs:
        client.rpcs = rpcs
    return client.call(method, params, timeout)


def get_native_balance(rpcs, addr):
    try:
        result, _ = rpc_call(rpcs, "eth_getBalance", [addr, "latest"], timeout=10)
        return int(result or "0x0", 16) / 1e18
    except Exception:
        return 0


def get_nonce(rpcs, addr):
    try:
        result, _ = rpc_call(rpcs, "eth_getTransactionCount", [addr, "latest"], timeout=10)
        return int(result or "0x0", 16)
    except Exception:
        return 0


def get_bnb_price():
    """获取 BNB 价格 — 复用 data_source PriceProvider（降级链: Binance -> DexScreener -> 600）"""
    try:
        return get_price_provider().get_bnb_price()
    except Exception:
        return 600


# token0 缓存（pair -> token0 address）
_token0_cache = {}

# token decimals 缓存（contract -> decimals）
_decimals_cache = {}


def get_token_decimals(contract, rpcs):
    """查询 ERC20 token 的 decimals（带缓存），默认 18"""
    contract = contract.lower()
    if contract in _decimals_cache:
        return _decimals_cache[contract]
    try:
        # decimals() selector = 0x313ce567
        result, _ = rpc_call(rpcs, "eth_call", [{"to": contract, "data": "0x313ce567"}, "latest"], timeout=10)
        if result and result != "0x" and len(result) >= 3:
            decimals = int(result, 16)
            if 0 <= decimals <= 36:
                _decimals_cache[contract] = decimals
                return decimals
    except Exception:
        pass
    _decimals_cache[contract] = 18
    return 18


def get_token0(pair, rpcs):
    """查询 pair 合约的 token0 地址（带缓存）"""
    if pair in _token0_cache:
        return _token0_cache[pair]
    try:
        # token0() selector = 0x0dfe1681
        result, _ = rpc_call(rpcs, "eth_call", [{"to": pair, "data": "0x0dfe1681"}, "latest"], timeout=10)
        if result and len(result) >= 42:
            token0 = "0x" + result[-40:]
            _token0_cache[pair] = token0
            return token0
    except Exception:
        pass
    return None


def get_reserves(pair, rpcs, token_decimals=18):
    """返回 (bnb_reserve, token_reserve)，通过查询 token0 确定哪一侧是 WBNB"""
    try:
        result, _ = rpc_call(rpcs, "eth_call", [{"to": pair, "data": "0x0902f1ac"}, "latest"], timeout=10)
        res = result or "0x"
        if res and len(res) >= 130:
            r0_raw = int(res[2:66], 16)
            r1_raw = int(res[66:130], 16)
            # 查询 token0 确定哪侧是 WBNB
            token0 = get_token0(pair, rpcs)
            if token0 and token0.lower() == WBNB:
                # token0 是 WBNB(18 decimals)，token1 是 token
                return (r0_raw / 1e18, r1_raw / (10 ** token_decimals))
            else:
                # token1 是 WBNB(18 decimals)，token0 是 token
                return (r1_raw / 1e18, r0_raw / (10 ** token_decimals))
    except Exception:
        pass
    return (0, 0)


# DexScreener 价格缓存
_price_cache = {}
_PRICE_CACHE_TTL = 60  # 缓存60秒


def get_token_price(contract):
    """获取代币价格 — 复用 data_source PriceProvider（自带缓存+降级链）"""
    try:
        price, _ = get_price_provider().get_token_price(contract)
        return price
    except Exception:
        pass
    return 0


def get_latest_block(rpcs):
    """获取最新区块号 — 复用 data_source RPCClient"""
    try:
        client = get_rpc_client()
        block = client.get_latest_block()
        return block, None
    except Exception:
        return 0, None


def get_logs(rpcs, from_block, to_block, address, topics):
    try:
        result, rpc = rpc_call(
            rpcs,
            "eth_getLogs",
            [{
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": address,
                "topics": topics,
            }],
            timeout=15,
        )
        return result or [], rpc
    except Exception:
        return [], None


def get_log_path(ts=None):
    dt = datetime.fromtimestamp(ts or time.time())
    return os.path.join(LOG_DIR, f"monitor-{dt.strftime('%Y-%m-%d')}.jsonl")


def get_recent_log_paths(days=2):
    paths = []
    today = datetime.now()
    for i in range(max(days, 1)):
        dt = today - timedelta(days=i)
        path = os.path.join(LOG_DIR, f"monitor-{dt.strftime('%Y-%m-%d')}.jsonl")
        if os.path.exists(path):
            paths.append(path)
    return paths


def prune_old_logs(retention_days=7):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        cutoff = datetime.now() - timedelta(days=max(retention_days, 1))
        for name in os.listdir(LOG_DIR):
            if not (name.startswith("monitor-") and name.endswith(".jsonl")):
                continue
            try:
                day_str = name[len("monitor-"):-len(".jsonl")]
                dt = datetime.strptime(day_str, "%Y-%m-%d")
                if dt < cutoff:
                    os.remove(os.path.join(LOG_DIR, name))
            except Exception:
                continue
    except Exception:
        pass


def log_event(event):
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = event.get("ts") or time.time()
    event["ts"] = ts
    event.setdefault("time", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)))
    path = get_log_path(ts)
    with open(path, "a") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def load_recent_events(contract_name, lookback_seconds=3600):
    events = []
    cutoff = time.time() - lookback_seconds
    for path in reversed(get_recent_log_paths(days=2)):
        try:
            with open(path) as f:
                for line in f:
                    try:
                        e = json.loads(line.strip())
                    except Exception:
                        continue
                    if e.get("contract") != contract_name:
                        continue
                    ts = e.get("ts")
                    if not isinstance(ts, (int, float)):
                        try:
                            ts = datetime.strptime(e.get("time", ""), "%Y-%m-%d %H:%M:%S").timestamp()
                        except Exception:
                            ts = None
                    if ts is None or ts < cutoff:
                        continue
                    e["time"] = ts
                    events.append(e)
        except Exception:
            continue
    return events


def update_wallet_stage(state, is_sell=False, is_transfer=False, is_watch_in=False, had_gas=False, had_nonce=False):
    current = state.get("stage", "静止")
    if is_sell:
        return "已出货"
    if is_transfer:
        return "已转仓"
    if had_nonce:
        return "已激活"
    if had_gas and current == "静止":
        return "观察中"
    if is_watch_in and current == "静止":
        return "观察中"
    return current


def format_watch_gas_alert(name, label, addr, bnb_balance):
    return (
        f"⚠️ <b>停放地址收到 Gas [{name}]</b>\n"
        f"地址: {short_addr(addr)}\n"
        f"标签: {label}\n"
        f"BNB余额: {bnb_balance:.6f}\n"
        f"阶段: 观察中\n"
        f"意义: 该地址可能即将启用"
    )


def format_watch_nonce_alert(name, label, addr, old_nonce, new_nonce):
    return (
        f"🚨 <b>停放地址已激活 [{name}]</b>\n"
        f"地址: {short_addr(addr)}\n"
        f"标签: {label}\n"
        f"Nonce: {old_nonce} → {new_nonce}\n"
        f"阶段: 已激活\n"
        f"意义: 该地址已开始主动发链上交易"
    )


def format_watch_transfer_alert(name, label, from_addr, to_addr, amount, usd, token_price, action, is_lp=False, stage="已转仓"):
    icon = "🚨" if is_lp else "⚠️"
    action_text = "开始出货(卖进LP)" if is_lp else ("转出" if action == "transfer" else action)
    extra = "\n去向: LP池子" if is_lp else f"\n去向: {short_addr(to_addr)}"
    value_line = f"价值: ${usd:,.0f}" if usd is not None else "价值: N/A（价格源暂不可用）"
    price_line = f"价格: ${token_price:.8f}" if token_price is not None else "价格: N/A"
    return (
        f"{icon} <b>停放地址异动 [{name}]</b>\n"
        f"地址: {short_addr(from_addr)}\n"
        f"标签: {label}\n"
        f"阶段: {stage}\n"
        f"动作: {action_text}{extra}\n"
        f"数量: {amount:,.0f}\n"
        f"{value_line}\n"
        f"{price_line}"
    )


def format_rpc_blind_alert(active_rpc, last_seen_block, blind_seconds):
    active = active_rpc or "none"
    return (
        "🚨 <b>监控数据源失明</b>\n"
        f"RPC: {active}\n"
        f"最后看到区块: {last_seen_block}\n"
        f"持续时长: {blind_seconds}s\n"
        "意义: 当前无法确认是否还在持续获取新区块"
    )


def format_rpc_recovered_alert(active_rpc, current_block):
    active = active_rpc or "unknown"
    return (
        "✅ <b>监控数据源恢复</b>\n"
        f"RPC: {active}\n"
        f"当前区块: {current_block}"
    )


def choose_scan_chunk(backlog, config):
    live_chunk = int(config.get("live_chunk_size", 200) or 200)
    catchup_chunk = int(config.get("catchup_chunk_size", 1000) or 1000)
    max_catchup_chunk = int(config.get("max_catchup_chunk_size", 3000) or 3000)

    if backlog > max_catchup_chunk * 5:
        return max_catchup_chunk
    if backlog > catchup_chunk:
        return catchup_chunk
    return live_chunk


def run_monitor(config, notify_fn):
    """主监控循环"""
    from notify import format_whale_alert, format_large_trade, format_lp_change
    from whale_alert import analyze_whale_pattern, format_alert

    try:
        from wechat_bridge import push_alert
        def notify_all(msg):
            notify_fn(msg)
            push_alert(msg)
    except ImportError:
        notify_all = notify_fn

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    config_mtime = os.path.getmtime(config_path) if os.path.exists(config_path) else 0
    rpcs = get_rpc_candidates(config)
    poll_interval = int(config.get("poll_interval", 30) or 30)
    rpc_stale_seconds = int(config.get("rpc_stale_seconds", 180) or 180)
    catchup_sleep_seconds = float(config.get("catchup_sleep_seconds", 1) or 1)
    log_retention_days = int(config.get("log_retention_days", 7) or 7)
    contracts = config.get("contracts", {})

    monitor_state = load_monitor_state()
    latest_block, active_rpc = get_latest_block(rpcs)
    saved_last_block = int(monitor_state.get("last_block", 0) or 0)

    if saved_last_block > 0 and latest_block > saved_last_block:
        last_block = saved_last_block
    elif latest_block > 10:
        last_block = latest_block - 10
    else:
        last_block = max(saved_last_block, 0)

    lp_state = {}
    bnb_price = get_bnb_price()
    price_update_time = time.time()
    watched_state = load_watched_state()

    last_progress_time = float(monitor_state.get("last_progress_time", time.time()) or time.time())
    last_seen_chain_block = int(monitor_state.get("last_seen_chain_block", latest_block or 0) or 0)
    rpc_stale_alerted = bool(monitor_state.get("rpc_stale_alerted", False))
    last_prune_time = float(monitor_state.get("last_prune_time", 0) or 0)

    # RPC 失败退避计数器
    consecutive_failures = 0

    if latest_block > last_seen_chain_block:
        last_seen_chain_block = latest_block
        last_progress_time = time.time()
        rpc_stale_alerted = False


    # WebSocket 实时模式
    monitor_mode = config.get("monitor_mode", "poll")
    ws_mon = None
    if monitor_mode == "ws":
        try:
            from ws_monitor import WsMonitor
            ws_mon = WsMonitor(config, list(contracts.keys()))
            if ws_mon.available:
                ws_mon.start()
                print(f"[monitor] WebSocket 模式启用，降级阈值 30s", file=sys.stderr)
            else:
                print(f"[monitor] 无 WSS 端点，降级为轮询", file=sys.stderr)
                ws_mon = None
        except ImportError:
            print(f"[monitor] ws_monitor 模块不可用，使用轮询", file=sys.stderr)

    print(f"[monitor] 启动监控 | {len(contracts)} 个合约 | 间隔 {poll_interval}s | 模式: {monitor_mode}", file=sys.stderr)
    print(f"[monitor] RPC candidates: {len(rpcs)} | active: {active_rpc}", file=sys.stderr)
    print(f"[monitor] 断点续跑: saved={saved_last_block} | start={last_block}", file=sys.stderr)
    print(f"[monitor] 起始区块: {last_block} | BNB: ${bnb_price:.2f}", file=sys.stderr)

    for contract, cfg in contracts.items():
        # 支持多 pair（pairs 数组）或单 pair（向后兼容）
        pairs_list = cfg.get("pairs", [])
        single_pair = cfg.get("pair", "")
        if not pairs_list and single_pair:
            pairs_list = [{"pair": single_pair, "dex": "PancakeSwap-v2"}]
        if not pairs_list:
            print(f"[monitor] 警告: 合约 {cfg.get('name', contract)} 缺少 pair 配置，跳过", file=sys.stderr)
            continue
        td = get_token_decimals(contract, rpcs)
        for pair_info in pairs_list:
            p = (pair_info["pair"] if isinstance(pair_info, dict) else pair_info).lower()
            dex = pair_info.get("dex", "unknown") if isinstance(pair_info, dict) else "unknown"
            reserves = get_reserves(p, rpcs, td)
            lp_state[p] = reserves
            print(f"[monitor] {cfg['name']} LP[{dex}]: {reserves[0]:.2f} BNB / {reserves[1]:,.0f} Token (decimals={td})", file=sys.stderr)

        for addr in cfg.get("watched_wallets", []):
            addr = addr.lower()
            watched_state[addr] = normalize_watch_state(watched_state.get(addr), time.time())
            if watched_state[addr]["bnb_balance"] == 0 and watched_state[addr]["nonce"] == 0:
                watched_state[addr]["bnb_balance"] = get_native_balance(rpcs, addr)
                watched_state[addr]["nonce"] = get_nonce(rpcs, addr)

    save_watched_state(watched_state)
    save_monitor_state({
        "last_block": last_block,
        "last_seen_chain_block": last_seen_chain_block,
        "last_progress_time": last_progress_time,
        "rpc_stale_alerted": rpc_stale_alerted,
        "active_rpc": active_rpc,
        "last_prune_time": last_prune_time,
        "updated_at": time.time(),
    })

    # 预警检查时间戳
    _last_alert_check = {}

    while True:
        try:
            now = time.time()

            try:
                latest_mtime = os.path.getmtime(config_path) if os.path.exists(config_path) else 0
            except Exception:
                latest_mtime = config_mtime
            if latest_mtime > config_mtime:
                runtime_config = load_runtime_config(config_path)
                if isinstance(runtime_config, dict):
                    config = runtime_config
                    config_mtime = latest_mtime
                    rpcs = get_rpc_candidates(config)
                    poll_interval = int(config.get("poll_interval", 30) or 30)
                    rpc_stale_seconds = int(config.get("rpc_stale_seconds", 180) or 180)
                    catchup_sleep_seconds = float(config.get("catchup_sleep_seconds", 1) or 1)
                    log_retention_days = int(config.get("log_retention_days", 7) or 7)
                    contracts = apply_config_reload(config, watched_state, lp_state, rpcs)
                    print(f"[monitor] 配置热重载完成 | 合约数: {len(contracts)}", file=sys.stderr)
            if now - price_update_time > 300:
                bnb_price = get_bnb_price()
                price_update_time = now

            if now - last_prune_time > 3600:
                prune_old_logs(log_retention_days)
                last_prune_time = now

            for contract, cfg in contracts.items():
                name = cfg["name"]
                labels = {k.lower(): v for k, v in cfg.get("watched_labels", {}).items()}
                for addr in cfg.get("watched_wallets", []):
                    addr = addr.lower()
                    label = labels.get(addr, "分仓停放地址")
                    state = normalize_watch_state(watched_state.get(addr), now)
                    watched_state[addr] = state
                    new_bnb = get_native_balance(rpcs, addr)
                    new_nonce = get_nonce(rpcs, addr)

                    # Gas 告警：检测余额增量超过阈值（如 0.01 BNB）
                    bnb_increase = new_bnb - state.get("bnb_balance", 0)
                    gas_threshold = 0.01  # BNB
                    if bnb_increase >= gas_threshold and not state.get("gas_alerted"):
                        state["stage"] = update_wallet_stage(state, had_gas=True)
                        state["last_action"] = f"收到 Gas (+{bnb_increase:.4f} BNB)"
                        state["last_update"] = now
                        msg = format_watch_gas_alert(name, label, addr, new_bnb)
                        notify_all(msg)
                        log_event({
                            "contract": name,
                            "type": "watch_gas",
                            "addr": addr,
                            "label": label,
                            "bnb_balance": new_bnb,
                            "bnb_increase": round(bnb_increase, 6),
                            "stage": state["stage"],
                        })
                        state["gas_alerted"] = True

                    # Nonce 告警：检测任何 nonce 增长（地址激活）
                    old_nonce = state.get("nonce", 0)
                    if new_nonce > old_nonce:
                        state["stage"] = update_wallet_stage(state, had_nonce=True)
                        state["last_action"] = f"发交易 (nonce {old_nonce}->{new_nonce})"
                        state["last_update"] = now
                        msg = format_watch_nonce_alert(name, label, addr, old_nonce, new_nonce)
                        notify_all(msg)
                        log_event({
                            "contract": name,
                            "type": "watch_nonce",
                            "addr": addr,
                            "label": label,
                            "old_nonce": old_nonce,
                            "new_nonce": new_nonce,
                            "stage": state["stage"],
                        })
                        state["nonce_alerted"] = True

                    state["bnb_balance"] = new_bnb
                    state["nonce"] = new_nonce

            current_block, active_rpc = get_latest_block(rpcs)

            if current_block > 0:
                consecutive_failures = 0
                if current_block > last_seen_chain_block:
                    last_seen_chain_block = current_block
                    last_progress_time = now
                    if rpc_stale_alerted:
                        notify_all(format_rpc_recovered_alert(active_rpc, current_block))
                        rpc_stale_alerted = False
                elif now - last_progress_time >= rpc_stale_seconds and not rpc_stale_alerted:
                    notify_all(format_rpc_blind_alert(active_rpc, last_seen_chain_block, int(now - last_progress_time)))
                    rpc_stale_alerted = True
            elif now - last_progress_time >= rpc_stale_seconds and not rpc_stale_alerted:
                notify_all(format_rpc_blind_alert(active_rpc, last_seen_chain_block, int(now - last_progress_time)))
                rpc_stale_alerted = True

            if current_block <= last_block:
                save_watched_state(watched_state)
                save_monitor_state({
                    "last_block": last_block,
                    "last_seen_chain_block": last_seen_chain_block,
                    "last_progress_time": last_progress_time,
                    "rpc_stale_alerted": rpc_stale_alerted,
                    "active_rpc": active_rpc,
                    "last_prune_time": last_prune_time,
                    "updated_at": now,
                })
                time.sleep(poll_interval)
                continue

            backlog = current_block - last_block
            chunk_size = choose_scan_chunk(backlog, config)
            scan_to = min(current_block, last_block + chunk_size)
            catchup_mode = (current_block - scan_to) > 0

            if catchup_mode:
                print(f"[monitor] catch-up | backlog={backlog} | chunk={chunk_size} | {last_block + 1}->{scan_to}", file=sys.stderr)

            # WSS 模式：在合约循环外一次性取所有 logs，避免 drain_logs 只在第一个合约生效
            all_ws_logs = []
            if ws_mon and ws_mon.connected and not ws_mon.should_fallback_to_poll():
                all_ws_logs = ws_mon.drain_logs()

            for contract, cfg in contracts.items():
                name = cfg["name"]
                # 构建所有 pair 地址集合（多 DEX 支持）
                pairs_list = cfg.get("pairs", [])
                single_pair = cfg.get("pair", "")
                if not pairs_list and single_pair:
                    pairs_list = [{"pair": single_pair, "dex": "PancakeSwap-v2"}]
                if not pairs_list:
                    continue
                pair_set = set()
                for pi in pairs_list:
                    p = (pi["pair"] if isinstance(pi, dict) else pi).lower()
                    pair_set.add(p)
                # 兼容：pair_lower 取第一个 pair（用于 LP 监控等）
                pair_lower = list(pair_set)[0]
                whale_set = {a.lower() for a in cfg.get("whale_addrs", [])}
                watched_set = {a.lower() for a in cfg.get("watched_wallets", [])}
                watched_labels = {k.lower(): v for k, v in cfg.get("watched_labels", {}).items()}
                threshold = cfg.get("alert_threshold_usd", 500)
                token_price = get_token_price(contract)
                price_available = token_price > 0
                token_price_value = token_price if price_available else None

                token_decimals = get_token_decimals(contract, rpcs)

                # WSS 模式：从预取的 logs 中按合约地址过滤
                ws_logs = []
                for wl in all_ws_logs:
                    log_addr = (wl.get("address") or "").lower()
                    if log_addr == contract.lower():
                        log_block = int(wl.get("blockNumber", "0x0"), 16)
                        if last_block < log_block <= scan_to:
                            ws_logs.append(wl)

                if ws_logs:
                    logs = ws_logs
                    logs_rpc = "wss"
                else:
                    logs, logs_rpc = get_logs(rpcs, last_block + 1, scan_to, contract, [TRANSFER_TOPIC])

                if not price_available and logs:
                    log_event({
                        "contract": name,
                        "type": "price_unavailable",
                        "from_block": last_block + 1,
                        "to_block": scan_to,
                        "logs_count": len(logs),
                        "rpc": logs_rpc,
                    })

                for log in logs:
                    from_addr = "0x" + log["topics"][1][-40:]
                    to_addr = "0x" + log["topics"][2][-40:]
                    amount = int(log["data"], 16) / (10 ** token_decimals)
                    usd = amount * token_price if price_available else None
                    block = int(log["blockNumber"], 16)

                    is_buy = from_addr.lower() in pair_set
                    is_sell = to_addr.lower() in pair_set
                    is_whale_from = from_addr.lower() in whale_set
                    is_whale_to = to_addr.lower() in whale_set
                    is_watch_from = from_addr.lower() in watched_set
                    is_watch_to = to_addr.lower() in watched_set

                    event = {
                        "contract": name,
                        "block": block,
                        "from": from_addr,
                        "to": to_addr,
                        "amount": amount,
                        "usd": usd,
                        "price": token_price_value,
                        "price_available": price_available,
                        "rpc": logs_rpc,
                        "type": "buy" if is_buy else "sell" if is_sell else "transfer",
                    }

                    if is_watch_from:
                        label = watched_labels.get(from_addr.lower(), "分仓停放地址")
                        state = normalize_watch_state(watched_state.get(from_addr.lower()), now)
                        watched_state[from_addr.lower()] = state
                        stage = update_wallet_stage(state, is_sell=is_sell, is_transfer=not is_sell)
                        state["stage"] = stage
                        state["last_action"] = "卖进LP" if is_sell else "转出"
                        state["last_update"] = now
                        event["watched"] = True
                        event["label"] = label
                        event["stage"] = stage
                        log_event(event)
                        msg = format_watch_transfer_alert(
                            name, label, from_addr, to_addr, amount, usd, token_price_value,
                            "sell" if is_sell else "transfer", is_lp=is_sell, stage=stage,
                        )
                        notify_all(msg)

                    elif is_watch_to:
                        label = watched_labels.get(to_addr.lower(), "分仓停放地址")
                        state = normalize_watch_state(watched_state.get(to_addr.lower()), now)
                        watched_state[to_addr.lower()] = state
                        state["stage"] = update_wallet_stage(state, is_watch_in=True)
                        state["last_action"] = "收到代币"
                        state["last_update"] = now
                        event["watched_in"] = True
                        event["label"] = label
                        event["stage"] = state["stage"]
                        log_event(event)

                    elif is_whale_from or is_whale_to:
                        whale_addr = from_addr if is_whale_from else to_addr
                        action = "sell" if is_sell else "buy" if is_buy else "transfer"
                        event["whale"] = True
                        log_event(event)

                        if price_available and usd is not None and usd > 100:
                            msg = format_whale_alert(name, whale_addr, action, amount, usd, token_price)
                            notify_all(msg)

                    elif price_available and usd is not None and usd >= threshold:
                        event["large"] = True
                        log_event(event)
                        action = "buy" if is_buy else "sell"
                        addr = to_addr if is_buy else from_addr
                        msg = format_large_trade(name, action, amount, usd, token_price, addr)
                        notify_all(msg)

                    else:
                        log_event(event)

                # LP 储备变化监控 — 仅在当前区间有事件时才查询（节省 RPC）
                if logs:  # 当前区间有事件才查 LP
                    for pi in pairs_list:
                        p = (pi["pair"] if isinstance(pi, dict) else pi).lower()
                        dex_label = pi.get("dex", "") if isinstance(pi, dict) else ""
                        new_reserves = get_reserves(p, rpcs, token_decimals)
                        old_reserves = lp_state.get(p, (0, 0))

                        if old_reserves[0] > 0 and new_reserves[0] > 0:
                            bnb_change_pct = abs(new_reserves[0] - old_reserves[0]) / old_reserves[0] * 100
                            if bnb_change_pct > 5:
                                lp_name = f"{name}[{dex_label}]" if dex_label else name
                                msg = format_lp_change(lp_name, old_reserves[0], new_reserves[0], old_reserves[1], new_reserves[1], bnb_price)
                                notify_all(msg)
                                log_event({
                                    "contract": name,
                                    "type": "lp_change",
                                    "dex": dex_label,
                                    "pair": p,
                                    "old_bnb": old_reserves[0],
                                    "new_bnb": new_reserves[0],
                                    "change_pct": bnb_change_pct,
                                })

                        lp_state[p] = new_reserves

                last_check = _last_alert_check.get(contract, 0)
                if price_available and time.time() - last_check > 300:
                    try:
                        recent_events = load_recent_events(name, lookback_seconds=3600)
                        alerts = analyze_whale_pattern(recent_events, whale_set, pair_lower, token_price)
                        for alert in alerts:
                            msg = format_alert(alert, name)
                            notify_all(msg)
                            log_event({
                                "contract": name,
                                "type": "alert",
                                "alert": alert,
                            })
                    except Exception as e:
                        print(f"[monitor] 预警检测错误: {e}", file=sys.stderr)
                    _last_alert_check[contract] = time.time()

            last_block = scan_to
            save_watched_state(watched_state)
            save_monitor_state({
                "last_block": last_block,
                "last_seen_chain_block": last_seen_chain_block,
                "last_progress_time": last_progress_time,
                "rpc_stale_alerted": rpc_stale_alerted,
                "active_rpc": active_rpc,
                "last_prune_time": last_prune_time,
                "updated_at": now,
            })

            if catchup_mode:
                time.sleep(catchup_sleep_seconds)
            else:
                time.sleep(poll_interval)

        except KeyboardInterrupt:
            print("\n[monitor] 停止", file=sys.stderr)
            if ws_mon:
                ws_mon.stop()
            save_watched_state(watched_state)
            save_monitor_state({
                "last_block": last_block,
                "last_seen_chain_block": last_seen_chain_block,
                "last_progress_time": last_progress_time,
                "rpc_stale_alerted": rpc_stale_alerted,
                "active_rpc": active_rpc,
                "last_prune_time": last_prune_time,
                "updated_at": time.time(),
            })
            break
        except Exception as e:
            consecutive_failures += 1
            backoff = min(10 * (2 ** (consecutive_failures - 1)), 300)
            print(f"[monitor] 错误 (#{consecutive_failures}): {e} | 退避 {backoff}s", file=sys.stderr)
            save_watched_state(watched_state)
            save_monitor_state({
                "last_block": last_block,
                "last_seen_chain_block": last_seen_chain_block,
                "last_progress_time": last_progress_time,
                "rpc_stale_alerted": rpc_stale_alerted,
                "active_rpc": active_rpc,
                "last_prune_time": last_prune_time,
                "updated_at": time.time(),
            })
            time.sleep(backoff)
