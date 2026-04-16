#!/usr/bin/env python3
"""
scan_core.py — 扒庄核心分析引擎
返回结构化数据，供 quick_scan.py (CLI) 和微信调用
"""
import sys, os, json, time, requests, sqlite3
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, timezone

# 统一数据源层
from data_source import AlchemyClient, PriceProvider, RPCClient, get_alchemy_client, get_price_provider, get_rpc_client

ZERO = "0x0000000000000000000000000000000000000000"
DEAD = "0x000000000000000000000000000000000000dead"
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
ALCHEMY_KEY = os.environ.get("ALCHEMY_KEY", "")
def _get_alchemy_url():
    key = os.environ.get("ALCHEMY_KEY", ALCHEMY_KEY)
    return f"https://bnb-mainnet.g.alchemy.com/v2/{key}"

# ═══════════════════════════════════════════
# 基础工具
# ═══════════════════════════════════════════

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
    except json.JSONDecodeError as e:
        print(f"[scan_core] config.json 格式错误: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[scan_core] 读取 config 失败: {e}", file=sys.stderr)

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
                "jsonrpc": "2.0", "method": method,
                "params": params, "id": 1
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


def eth_call(to, data):
    try:
        result, _ = rpc_call("eth_call", [{"to": to, "data": data}, "latest"], timeout=10)
        return result or "0x"
    except Exception:
        return "0x"

def decode_string(hex_str):
    if not hex_str or hex_str == "0x":
        return "?"
    try:
        hex_str = hex_str[2:]
        offset = int(hex_str[0:64], 16) * 2
        length = int(hex_str[offset:offset+64], 16)
        return bytes.fromhex(hex_str[offset+64:offset+64+length*2]).decode("utf-8", errors="replace")
    except Exception:
        return "?"

def get_bnb_price():
    """获取 BNB 价格 — 复用统一数据源（降级链: Binance → DexScreener → 硬编码）"""
    try:
        return get_price_provider().get_bnb_price()
    except Exception:
        return 600

def get_token_price(contract):
    """获取代币价格 — 复用统一数据源（降级链: DexScreener → GeckoTerminal）"""
    try:
        return get_price_provider().get_token_price(contract)
    except Exception:
        pass
    return 0, {}

# ═══════════════════════════════════════════
# Token 基本信息
# ═══════════════════════════════════════════

def get_token_info(contract):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 并行查询 5 个基本字段
    def _query(data_sig):
        return data_sig, eth_call(contract, data_sig)

    basic_sigs = ["0x06fdde03", "0x95d89b41", "0x313ce567", "0x18160ddd", "0x8da5cb5b"]
    basic_results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_query, sig): sig for sig in basic_sigs}
        for future in as_completed(futures):
            sig, result = future.result()
            basic_results[sig] = result

    name = decode_string(basic_results.get("0x06fdde03", "0x"))
    symbol = decode_string(basic_results.get("0x95d89b41", "0x"))
    dec_hex = basic_results.get("0x313ce567", "0x")
    decimals = int(dec_hex, 16) if dec_hex and dec_hex != "0x" else 18
    supply_hex = basic_results.get("0x18160ddd", "0x")
    total_supply = int(supply_hex, 16) / (10 ** decimals) if supply_hex and supply_hex != "0x" else 0
    owner_hex = basic_results.get("0x8da5cb5b", "0x")
    owner = "0x" + owner_hex[-40:] if owner_hex and len(owner_hex) >= 42 else "?"

    WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
    pools = []  # [(address, version), ...]

    # ═══ 多 DEX V2 + V3 Factory 并行查询 ═══
    V2_FACTORIES = [
        ("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73", "PancakeSwap"),
        ("0x858E3312ed3A876947EA49d572A7C42DE08af7EE", "Biswap"),
        ("0x86407bEa2078ea5f5EB5A52B2caA963bC1F889Da", "BabySwap"),
        ("0x3CD1C46068dAEa5Ebb0d3f55F6915B10648062B8", "MDEX"),
    ]
    V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
    V3_FEES = [100, 500, 2500, 10000]

    def _query_factory(args):
        addr, name, data = args
        return name, eth_call(addr, data)

    factory_queries = []
    for factory_addr, dex_name in V2_FACTORIES:
        data = "0xe6a43905" + contract[2:].lower().zfill(64) + WBNB[2:].lower().zfill(64)
        factory_queries.append((factory_addr, f"{dex_name}-v2", data))
    for fee in V3_FEES:
        data = "0x1698ee82" + contract[2:].lower().zfill(64) + WBNB[2:].lower().zfill(64) + hex(fee)[2:].zfill(64)
        factory_queries.append((V3_FACTORY, f"v3-{fee}", data))

    factory_results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_query_factory, q): q[1] for q in factory_queries}
        for future in as_completed(futures):
            name_key, result = future.result()
            factory_results[name_key] = result

    v2_pair = None
    for factory_addr, dex_name in V2_FACTORIES:
        key = f"{dex_name}-v2"
        pair_hex = factory_results.get(key, "0x")
        if pair_hex and int(pair_hex, 16) != 0:
            found_pair = "0x" + pair_hex[-40:]
            if found_pair.lower() not in {p[0].lower() for p in pools}:
                pools.append((found_pair, key))
                if v2_pair is None:
                    v2_pair = found_pair

    for fee in V3_FEES:
        key = f"v3-{fee}"
        pool_hex = factory_results.get(key, "0x")
        if pool_hex and len(pool_hex) >= 42 and int(pool_hex, 16) != 0:
            pool_addr = "0x" + pool_hex[-40:]
            if pool_addr.lower() not in {p[0].lower() for p in pools}:
                pools.append((pool_addr, f"PancakeSwap-v3-{fee}"))

    # 主 pair 用于价格历史（优先 V2，没有则用第一个 V3）
    pair = v2_pair or (pools[0][0] if pools else None)

    return {
        "name": name, "symbol": symbol, "decimals": decimals,
        "total_supply": total_supply, "owner": owner,
        "pair": pair, "pools": pools,
    }

# ═══════════════════════════════════════════
# 转账拉取 + SQLite 缓存
# ═══════════════════════════════════════════

def _parse_transfer(tx, decimals=18):
    from_addr = (tx.get("from") or "").lower()
    to_addr = (tx.get("to") or "").lower()
    if not from_addr or not to_addr:
        return None
    value = tx.get("value")
    if value is not None:
        try:
            # Alchemy getAssetTransfers 的 value 已经是 token 数量（如 "1000.5"），
            # 不需要再除以 decimals，直接转 float 即可
            amount = float(Decimal(str(value)))
        except Exception:
            return None
    else:
        raw = tx.get("rawContract") or {}
        hex_val = raw.get("value") or "0x0"
        try:
            # rawContract.value 是 wei 格式的 hex，需要除以 decimals
            amount = int(hex_val, 16) / (10 ** decimals)
        except Exception:
            return None
    if amount <= 0:
        return None
    block = int(tx.get("blockNum") or "0x0", 16)
    return (block, from_addr, to_addr, amount)

# ═══ Alchemy getAssetTransfers 拉取（新增 — 替代 eth_getLogs 全量拉取）═══

def _parse_alchemy_transfer(tx, decimals=18):
    """解析 Alchemy getAssetTransfers 返回的单条转账记录
    输出格式与 _parse_log_transfer 一致: (block, from_addr, to_addr, amount)
    """
    from_addr = (tx.get("from") or "").lower()
    to_addr = (tx.get("to") or "").lower()
    if not from_addr or not to_addr:
        return None, None

    # 解析金额
    value = tx.get("value")
    if value is not None:
        try:
            # Alchemy getAssetTransfers 的 value 已经是 token 数量，不需要再除以 decimals
            amount = float(Decimal(str(value)))
        except Exception:
            return None, None
    else:
        raw = tx.get("rawContract") or {}
        hex_val = raw.get("value") or "0x0"
        try:
            amount = int(hex_val, 16) / (10 ** decimals)
        except Exception:
            return None, None

    if amount <= 0:
        return None, None

    block = int(tx.get("blockNum") or "0x0", 16)
    tx_hash = tx.get("hash", "")
    return (block, from_addr, to_addr, amount), tx_hash


def _full_fetch_via_alchemy(contract, conn, decimals=18, progress_fn=None):
    """全量拉取 — 用 Alchemy getAssetTransfers 分页（替代 eth_getLogs 并发拉取）
    优势: 一次请求返回最多 1000 条转账，无需逐块扫描，速度提升 10-50x
    """
    if progress_fn is None:
        progress_fn = lambda msg: print(msg, file=sys.stderr)

    client = get_alchemy_client()
    if not client.available:
        progress_fn("  Alchemy key 不可用，回退到 eth_getLogs")
        return _full_fetch_to_db(contract, conn, decimals)

    # 检查是否有部分数据可以跳过
    existing_max = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or 0
    existing_count = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]

    if existing_max > 0:
        from_block_hex = hex(existing_max)
        progress_fn(f"  续传: 已有 {existing_count:,} 笔, 从 block {existing_max} 继续 (Alchemy)")
    else:
        from_block_hex = "0x0"
        progress_fn(f"  全量拉取 (Alchemy getAssetTransfers)...")

    page_key = None
    total_new = 0
    page_count = 0
    batch_size = int(_load_config_ds().get("data_source", {}).get("history", {}).get("alchemy_page_size", 1000))
    sleep_between = float(_load_config_ds().get("data_source", {}).get("history", {}).get("alchemy_sleep_between_pages", 0.2))

    while True:
        try:
            transfers, next_page_key = client.get_asset_transfers(
                contract,
                from_block=from_block_hex,
                max_count=batch_size,
                page_key=page_key,
                order="asc",
            )
        except Exception as e:
            progress_fn(f"  Alchemy 请求失败: {e}, 回退到 eth_getLogs")
            return _full_fetch_to_db(contract, conn, decimals)

        if not transfers:
            break

        # 解析并写入 DB
        records = []
        hashes = []
        for tx in transfers:
            rec, tx_hash = _parse_alchemy_transfer(tx, decimals)
            if rec:
                records.append(rec)
                hashes.append(tx_hash)

        if records:
            # 带 tx_hash 写入
            batch_data = [(r[0], r[1], r[2], r[3], h) for r, h in zip(records, hashes)]
            conn.executemany("INSERT OR IGNORE INTO transfers (block, from_addr, to_addr, amount, tx_hash) VALUES (?,?,?,?,?)", batch_data)
            conn.commit()
            total_new += len(records)

        page_count += 1
        page_key = next_page_key

        if page_count % 50 == 0:
            progress_fn(f"  Alchemy 进度: {page_count} 页, +{total_new:,} 条")

        if not page_key:
            break

        time.sleep(sleep_between)

    # 更新 sync_state
    max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or 0
    actual_total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    conn.execute("INSERT OR REPLACE INTO sync_state (id, page_key, total, last_synced_block, last_sync_ts) VALUES (1, NULL, ?, ?, datetime('now'))",
                 (actual_total, max_block))
    conn.commit()
    progress_fn(f"  Alchemy 全量拉取完成: {actual_total:,} 笔 (+{total_new}), max_block={max_block}")
    return actual_total, None


def _incremental_fetch_via_alchemy(contract, from_block, conn, decimals=18, progress_fn=None):
    """增量同步 — 用 Alchemy getAssetTransfers（大范围增量，比 eth_getLogs 快得多）"""
    if progress_fn is None:
        progress_fn = lambda msg: print(msg, file=sys.stderr)

    client = get_alchemy_client()
    if not client.available:
        progress_fn("  Alchemy key 不可用，回退到 eth_getLogs")
        return _incremental_fetch_by_block(contract, from_block, conn, decimals)

    to_block = get_rpc_client().get_latest_block()
    if to_block <= from_block:
        return 0

    progress_fn(f"  增量同步 (Alchemy, block>{from_block})...")

    page_key = None
    total_new = 0
    batch_size = int(_load_config_ds().get("data_source", {}).get("history", {}).get("alchemy_page_size", 1000))
    sleep_between = float(_load_config_ds().get("data_source", {}).get("history", {}).get("alchemy_sleep_between_pages", 0.2))
    from_block_hex = hex(from_block)

    while True:
        try:
            transfers, next_page_key = client.get_asset_transfers(
                contract,
                from_block=from_block_hex,
                to_block="latest",
                max_count=batch_size,
                page_key=page_key,
                order="asc",
            )
        except Exception as e:
            progress_fn(f"  Alchemy 增量请求失败: {e}, 回退到 eth_getLogs")
            return _incremental_fetch_by_block(contract, from_block, conn, decimals)

        if not transfers:
            break

        records = []
        hashes = []
        for tx in transfers:
            rec, tx_hash = _parse_alchemy_transfer(tx, decimals)
            if rec:
                records.append(rec)
                hashes.append(tx_hash)

        if records:
            batch_data = [(r[0], r[1], r[2], r[3], h) for r, h in zip(records, hashes)]
            conn.executemany("INSERT OR IGNORE INTO transfers (block, from_addr, to_addr, amount, tx_hash) VALUES (?,?,?,?,?)", batch_data)
            conn.commit()
            total_new += len(records)

        page_key = next_page_key
        if not page_key:
            break
        time.sleep(sleep_between)

    # 更新 sync_state
    max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or from_block
    actual_total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    conn.execute("INSERT OR REPLACE INTO sync_state (id, page_key, total, last_synced_block, last_sync_ts) VALUES (1, NULL, ?, ?, datetime('now'))",
                 (actual_total, max_block))
    conn.commit()
    progress_fn(f"  Alchemy 增量完成: +{total_new} 条, max_block={max_block}")
    return total_new


def _load_config_ds():
    """读取 config.json 的 data_source 配置"""
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        if os.path.exists(config_path):
            with open(config_path) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


# ═══ eth_getLogs 并发拉取（保留作为 Alchemy 不可用时的降级方案）═══
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def _parse_log_transfer(log, decimals=18):
    """解析 eth_getLogs 返回的 Transfer event log"""
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None
    from_addr = "0x" + topics[1][-40:]
    to_addr = "0x" + topics[2][-40:]
    data = log.get("data", "0x0")
    try:
        amount = int(data, 16) / (10 ** decimals)
    except Exception:
        return None
    if amount <= 0:
        return None
    block = int(log.get("blockNumber", "0x0"), 16)
    return (block, from_addr.lower(), to_addr.lower(), amount)

def _fetch_logs_chunk(rpc, contract, from_block, to_block):
    """单个 chunk 的 eth_getLogs 请求"""
    try:
        r = requests.post(rpc, json={
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": contract.lower(),
                "topics": [TRANSFER_TOPIC],
            }],
            "id": 1
        }, timeout=30)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            return None, str(data["error"])
        result = data.get("result", [])
        if isinstance(result, list):
            return result, None
        return None, "invalid result"
    except Exception as e:
        return None, str(e)

def _fetch_logs_chunk_with_retry(contract, from_block, to_block, rpcs):
    """带 RPC 轮询重试的 chunk 拉取"""
    for rpc in rpcs:
        logs, err = _fetch_logs_chunk(rpc, contract, from_block, to_block)
        if logs is not None:
            return logs
    return []

def _get_latest_block(rpcs):
    """获取最新区块号"""
    for rpc in rpcs:
        try:
            r = requests.post(rpc, json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}, timeout=10)
            return int(r.json().get("result", "0x0"), 16)
        except Exception:
            continue
    return 0

def _find_first_transfer_block(contract, rpcs, latest_block):
    """快速查找合约首笔 Transfer 区块 — 复用 data_source AlchemyClient"""
    # 方法1: Alchemy getAssetTransfers 查第一条
    try:
        client = get_alchemy_client()
        transfers, _ = client.get_asset_transfers(
            contract, from_block="0x0", max_count=1, order="asc"
        )
        if transfers:
            first = int(transfers[0].get("blockNum", "0x0"), 16)
            return max(first - 100, 0)
    except Exception:
        pass

    # 方法2: 二分查找（粗粒度，每次跨 2M block）
    rpc_client = get_rpc_client()
    for b in range(0, latest_block, 2000000):
        end = min(b + 2000000 - 1, latest_block)
        logs = rpc_client.get_logs(contract, b, end, TRANSFER_TOPIC)
        if logs:
            first = int(logs[0].get("blockNumber", "0x0"), 16)
            return max(first - 100, 0)

    return max(latest_block - 50000, 0)

def _fetch_logs_parallel(contract, from_block, to_block, rpcs, decimals=18, conn=None, progress_fn=None, chunk_size=2000, workers=6):
    """并发拉取 eth_getLogs，返回新增记录数"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if progress_fn is None:
        progress_fn = lambda msg: print(msg, file=sys.stderr)

    total_new = 0
    chunks = []
    for start in range(from_block, to_block + 1, chunk_size):
        end = min(start + chunk_size - 1, to_block)
        chunks.append((start, end))

    if not chunks:
        return 0

    progress_fn(f"  拉取 {len(chunks)} 个 chunk ({from_block} -> {to_block}), {workers} 并发")

    # 分配 RPC: 轮询分配给不同 worker
    def fetch_chunk(idx_chunk):
        idx, (start, end) = idx_chunk
        # 每个 chunk 用不同的起始 RPC 实现负载均衡
        rotated_rpcs = rpcs[idx % len(rpcs):] + rpcs[:idx % len(rpcs)]
        logs = _fetch_logs_chunk_with_retry(contract, start, end, rotated_rpcs)
        records = []
        for log in logs:
            rec = _parse_log_transfer(log, decimals)
            if rec:
                records.append(rec)
        return records

    batch_buffer = []
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_chunk, (i, c)): i for i, c in enumerate(chunks)}
        for future in as_completed(futures):
            try:
                records = future.result()
                batch_buffer.extend(records)
            except Exception as e:
                print(f"  chunk error: {e}", file=sys.stderr)
            completed += 1
            # 每 5000 条或每 200 chunk 写入一次 DB
            if len(batch_buffer) >= 5000 or completed == len(chunks):
                if batch_buffer and conn:
                    conn.executemany("INSERT OR IGNORE INTO transfers (block, from_addr, to_addr, amount) VALUES (?,?,?,?)", batch_buffer)
                    conn.commit()
                    total_new += len(batch_buffer)
                    batch_buffer = []
            if completed % 200 == 0 or completed == len(chunks):
                progress_fn(f"  进度: {completed}/{len(chunks)} chunks, +{total_new:,} 条")

    return total_new

def _init_db(cache_db):
    conn = sqlite3.connect(cache_db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS transfers (
        block INTEGER, from_addr TEXT, to_addr TEXT, amount REAL,
        tx_hash TEXT DEFAULT '')""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY, page_key TEXT, total INTEGER,
        last_synced_block INTEGER DEFAULT 0, last_sync_ts TEXT DEFAULT '')""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_from ON transfers(from_addr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_to ON transfers(to_addr)")
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup ON transfers(block, from_addr, to_addr, amount)")
    except sqlite3.IntegrityError:
        # 老数据有重复，先去重再建索引
        print("  [sync] 清理重复数据...", file=sys.stderr)
        conn.execute("""DELETE FROM transfers WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM transfers GROUP BY block, from_addr, to_addr, amount)""")
        conn.commit()
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup ON transfers(block, from_addr, to_addr, amount)")
    conn.commit()
    # 老库迁移: 检测并添加新列
    try:
        conn.execute("SELECT last_synced_block FROM sync_state LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE sync_state ADD COLUMN last_synced_block INTEGER DEFAULT 0")
        conn.execute("ALTER TABLE sync_state ADD COLUMN last_sync_ts TEXT DEFAULT ''")
        max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0]
        if max_block:
            conn.execute("UPDATE sync_state SET last_synced_block = ?", (max_block,))
        conn.commit()
        print(f"  [sync] 数据库迁移完成, last_synced_block={max_block}", file=sys.stderr)
    return conn

def _full_fetch_to_db(contract, conn, decimals=18):
    """全量拉取 — 用 eth_getLogs 并发"""
    rpcs = RPC_CANDIDATES
    progress_fn = lambda msg: print(msg, file=sys.stderr)

    # 检查是否有部分数据可以跳过
    existing_max = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or 0
    existing_count = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]

    to_block = _get_latest_block(rpcs)
    if existing_max > 0:
        from_block = existing_max
        progress_fn(f"  续传: 已有 {existing_count:,} 笔, 从 block {from_block} 继续")
    else:
        # 二分查找合约首笔 Transfer 的区块
        progress_fn(f"  查找首笔交易区块...")
        from_block = _find_first_transfer_block(contract, rpcs, to_block)
        progress_fn(f"  从 block {from_block} 开始全量拉取")

    if to_block == 0:
        progress_fn("  无法获取最新区块")
        return 0, None

    total_new = _fetch_logs_parallel(contract, from_block, to_block, rpcs, decimals, conn, progress_fn)

    # 更新 sync_state
    max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or 0
    actual_total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    conn.execute("INSERT OR REPLACE INTO sync_state (id, page_key, total, last_synced_block, last_sync_ts) VALUES (1, NULL, ?, ?, datetime('now'))",
                 (actual_total, max_block))
    conn.commit()
    progress_fn(f"  全量拉取完成: {actual_total:,} 笔 (+{total_new}), max_block={max_block}")
    return actual_total, None

def _incremental_fetch_by_block(contract, from_block, conn, decimals=18):
    """基于区块号的快速增量同步 — eth_getLogs 并发"""
    rpcs = RPC_CANDIDATES
    to_block = _get_latest_block(rpcs)
    if to_block <= from_block:
        return 0

    progress_fn = lambda msg: print(msg, file=sys.stderr)
    block_gap = to_block - from_block

    # 小增量（<5000 block）用单线程，大增量用并发
    if block_gap < 5000:
        workers = 2
        chunk_size = 2000
    elif block_gap < 50000:
        workers = 4
        chunk_size = 2000
    else:
        workers = 6
        chunk_size = 2000

    new = _fetch_logs_parallel(contract, from_block, to_block, rpcs, decimals, conn, progress_fn, chunk_size, workers)

    # 更新 sync_state
    max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or from_block
    actual_total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    conn.execute("INSERT OR REPLACE INTO sync_state (id, page_key, total, last_synced_block, last_sync_ts) VALUES (1, NULL, ?, ?, datetime('now'))",
                 (actual_total, max_block))
    conn.commit()
    return new

def _load_from_db(conn):
    return conn.execute("SELECT block, from_addr, to_addr, amount FROM transfers ORDER BY block ASC").fetchall()

def load_transfers(contract, progress_fn=None, decimals=18, skip_sync=False, max_age_seconds=300):
    """加载转账数据（自动缓存 + 智能增量同步）。
    skip_sync: 跳过同步，直接用缓存
    max_age_seconds: 最近N秒内同步过则自动跳过（默认5分钟）
    返回 [(block, from, to, amount), ...]
    """
    contract = contract.lower()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cache_db = os.path.join(script_dir, "data", f"{contract[:20]}.db")
    os.makedirs(os.path.dirname(cache_db), exist_ok=True)
    if progress_fn is None:
        progress_fn = lambda msg: print(msg, file=sys.stderr)

    if os.path.exists(cache_db):
        conn = _init_db(cache_db)  # _init_db 会自动迁移老库
        try:
            cached = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
            row = conn.execute("SELECT page_key, last_synced_block, last_sync_ts FROM sync_state WHERE id=1").fetchone()
            pk = row[0] if row else None
            last_block = (row[1] or 0) if row else 0
            last_ts = (row[2] or "") if row else ""

            # 跳过同步
            if skip_sync:
                progress_fn(f"缓存: {cached:,} 笔 (跳过同步)")
                return _load_from_db(conn)

            # 自动跳过: 最近同步过
            if last_ts and max_age_seconds > 0:
                try:
                    from datetime import datetime, timezone
                    synced_at = datetime.fromisoformat(last_ts)
                    if synced_at.tzinfo is None:
                        synced_at = synced_at.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - synced_at).total_seconds()
                    if age < max_age_seconds:
                        progress_fn(f"缓存: {cached:,} 笔 ({int(age)}秒前已同步)")
                        return _load_from_db(conn)
                except Exception:
                    pass

            # 路径A: last_synced_block > 0 → 智能增量同步
            if last_block > 0:
                # 获取当前最新区块，判断增量大小
                _rpc_client = get_rpc_client()
                current_block = _rpc_client.get_latest_block()
                gap = current_block - last_block if current_block > last_block else 0

                # 读取数据源策略配置
                ds_config = _load_config_ds().get("data_source", {}).get("history", {})
                primary_source = ds_config.get("primary", "alchemy_transfers")
                alchemy_threshold = int(ds_config.get("alchemy_threshold", 50000))

                # 智能模式选择: 大范围用 Alchemy，小范围用 eth_getLogs
                if primary_source == "alchemy_transfers" and gap > alchemy_threshold:
                    progress_fn(f"缓存: {cached:,} 笔, Alchemy增量同步 (block>{last_block}, gap={gap:,})...")
                    new = _incremental_fetch_via_alchemy(contract, last_block, conn, decimals, progress_fn)
                elif primary_source == "alchemy_transfers" and gap > 0:
                    progress_fn(f"缓存: {cached:,} 笔, 小增量同步 (block>{last_block}, gap={gap:,})...")
                    new = _incremental_fetch_by_block(contract, last_block, conn, decimals)
                else:
                    progress_fn(f"缓存: {cached:,} 笔, 增量同步 (block>{last_block})...")
                    new = _incremental_fetch_by_block(contract, last_block, conn, decimals)
                total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
                progress_fn(f"同步完成: {total:,} 笔 (+{new})")

            # 路径B: 有 page_key 但无 last_block → 恢复中断的全量拉取（优先 Alchemy）
            elif pk:
                ds_config = _load_config_ds().get("data_source", {}).get("history", {})
                if ds_config.get("primary", "alchemy_transfers") == "alchemy_transfers":
                    progress_fn(f"缓存: {cached:,} 笔, 恢复全量同步 (Alchemy)...")
                    _full_fetch_via_alchemy(contract, conn, decimals)
                else:
                    progress_fn(f"缓存: {cached:,} 笔, 恢复全量同步...")
                    _full_fetch_to_db(contract, conn, decimals)

            # 路径C: 有数据但无 state → 从 MAX(block) 回填，走增量
            elif cached > 0:
                max_b = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0]
                if max_b:
                    progress_fn(f"缓存: {cached:,} 笔, 修复状态后增量同步...")
                    conn.execute("INSERT OR REPLACE INTO sync_state (id, page_key, total, last_synced_block, last_sync_ts) VALUES (1, NULL, ?, ?, datetime('now'))",
                                 (cached, max_b))
                    conn.commit()
                    new = _incremental_fetch_by_block(contract, max_b, conn, decimals)
                    progress_fn(f"同步完成: +{new}")
                else:
                    progress_fn("缓存异常，重新全量拉取...")
                    _full_fetch_to_db(contract, conn, decimals)

            # 路径D: 完全没有数据 → 优先 Alchemy 全量拉取
            else:
                ds_config = _load_config_ds().get("data_source", {}).get("history", {})
                if ds_config.get("primary", "alchemy_transfers") == "alchemy_transfers":
                    progress_fn("首次拉取，全量同步中 (Alchemy)...")
                    _full_fetch_via_alchemy(contract, conn, decimals)
                else:
                    progress_fn("首次拉取，全量同步中...")
                    _full_fetch_to_db(contract, conn, decimals)

            return _load_from_db(conn)
        finally:
            conn.close()
    else:
        conn = _init_db(cache_db)
        try:
            ds_config = _load_config_ds().get("data_source", {}).get("history", {})
            if ds_config.get("primary", "alchemy_transfers") == "alchemy_transfers":
                progress_fn("首次拉取，全量同步中 (Alchemy)...")
                _full_fetch_via_alchemy(contract, conn, decimals)
            else:
                progress_fn("首次拉取，全量同步中...")
                _full_fetch_to_db(contract, conn, decimals)
            records = _load_from_db(conn)
        finally:
            conn.close()
        progress_fn(f"总计: {len(records):,} 笔转账（已缓存）")
        return records

# ═══════════════════════════════════════════
# 价格历史
# ═══════════════════════════════════════════

def get_price_history(pair_addr):
    candles = []
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/bsc/pools/{pair_addr}/ohlcv/hour?limit=1000"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            ohlcv = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
            for c in ohlcv:
                candles.append({"ts": c[0], "vwap": (float(c[1]) + float(c[2]) + float(c[3]) + float(c[4])) / 4})
    except Exception:
        pass
    return {c["ts"]: c["vwap"] for c in candles}

def get_block_ts_mapping(records):
    if not records:
        return 0.451, 0
    blocks = [records[0][0], records[len(records)//2][0], records[-1][0]]
    block_ts = {}
    for b in blocks:
        try:
            result, _ = rpc_call("eth_getBlockByNumber", [hex(b), False], timeout=10)
            ts_hex = (result or {}).get("timestamp")
            if ts_hex:
                block_ts[b] = int(ts_hex, 16)
        except Exception:
            pass
    if len(block_ts) < 2:
        return 0.451, 0
    items = sorted(block_ts.items())
    xs = [b for b, _ in items]
    ys = [t for _, t in items]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom_ss = sum((x - mean_x)**2 for x in xs)
    if denom_ss == 0:
        return 0.451, 0
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom_ss
    intercept = mean_y - slope * mean_x
    return slope, intercept

# ═══════════════════════════════════════════
# 庄家识别
# ═══════════════════════════════════════════

def is_contract_address(addr):
    """检查地址是否是合约（非 EOA）"""
    try:
        result, _ = rpc_call("eth_getCode", [addr, "latest"], timeout=5)
        code = result or "0x"
        return len(code) > 4
    except Exception:
        return False

def identify_whales(records, pools_info, total_supply, top_n=10):
    """pools_info: [(addr, version), ...] 或单个 pair 字符串（向后兼容）"""
    if isinstance(pools_info, str):
        pool_set = {pools_info.lower()}
    else:
        pool_set = {p[0].lower() for p in pools_info}
    exclude = {ZERO, DEAD} | pool_set
    buyers = defaultdict(float)
    balances = defaultdict(float)
    transfer_graph = defaultdict(lambda: defaultdict(float))

    for block, fa, ta, amount in records:
        balances[fa] -= amount
        balances[ta] += amount
        if fa in pool_set:
            buyers[ta] += amount
        elif fa == ZERO:
            # mint 事件（from=0x0）：to_addr 正常累加到 transfer_graph
            # 不追踪 from 方，但 to 方的余额来源应被记录
            pass  # balances 已经累加了 to_addr
        elif ta == ZERO or ta == DEAD:
            # burn 事件：from 的转出已通过 balances 扣减
            pass
        elif fa not in exclude and ta not in exclude:
            transfer_graph[fa][ta] += amount

    candidates = set()
    sorted_buyers = sorted(buyers.items(), key=lambda x: -x[1])
    for addr, amt in sorted_buyers[:top_n]:
        if addr not in exclude:
            candidates.add(addr)
    sorted_holders = sorted(
        [(k, v) for k, v in balances.items() if v > total_supply * 0.005 and k not in exclude],
        key=lambda x: -x[1]
    )
    for addr, _ in sorted_holders[:top_n]:
        candidates.add(addr)

    related = set()
    for addr in candidates:
        for dst, amt in transfer_graph.get(addr, {}).items():
            if amt > total_supply * 0.005 and dst not in exclude:
                related.add(dst)
        for src in transfer_graph:
            if addr in transfer_graph[src] and transfer_graph[src][addr] > total_supply * 0.005:
                if src not in exclude:
                    related.add(src)

    all_whales = candidates | related

    # 过滤掉合约地址（锁仓/质押/路由器等）
    contract_cache = {}
    filtered = set()
    for addr in all_whales:
        if addr not in contract_cache:
            contract_cache[addr] = is_contract_address(addr)
        if contract_cache[addr]:
            print(f"  排除合约地址: {addr[:10]}..{addr[-4:]} (持仓{balances.get(addr,0):,.0f})", file=sys.stderr)
        else:
            filtered.add(addr)

    whale_scores = {}
    for addr in filtered:
        score = buyers.get(addr, 0) + max(balances.get(addr, 0), 0)
        whale_scores[addr] = score
    sorted_whales = sorted(whale_scores.items(), key=lambda x: -x[1])
    return [addr for addr, _ in sorted_whales[:20]]

# ═══════════════════════════════════════════
# 核心分析（返回结构化数据）
# ═══════════════════════════════════════════

def run_analysis(contract_addr, progress_fn=None, deep=False, skip_sync=False):
    """
    核心分析入口。返回结构化 dict，不做任何 print。
    progress_fn: 可选的进度回调 fn(msg)
    deep: 是否启用深度分析（跨合约追踪，耗时较长）
    """
    contract = contract_addr.lower()

    # 合约地址格式校验
    if not contract.startswith("0x") or len(contract) != 42:
        return {"error": f"无效合约地址格式: {contract_addr}（需要 0x + 40 hex 字符）"}
    try:
        int(contract[2:], 16)
    except ValueError:
        return {"error": f"合约地址包含非十六进制字符: {contract_addr}"}

    if not os.environ.get("ALCHEMY_KEY", ""):
        return {"error": "请设置 ALCHEMY_KEY 环境变量"}

    if progress_fn is None:
        progress_fn = lambda msg: print(msg, file=sys.stderr)

    # 基本信息
    progress_fn("📋 查询 Token 信息...")
    info = get_token_info(contract_addr)
    tp, dex_data = get_token_price(contract_addr)
    bnb_price = get_bnb_price()
    total_supply = info["total_supply"]
    pair = info["pair"]

    if not pair:
        return {"error": "未找到 PancakeSwap LP pair (V2/V3)"}

    # 构建 pool 集合（V2 + V3）
    pools = info.get("pools", [])
    if not pools and pair:
        pools = [(pair, "v2")]
    pool_set = {p[0].lower() for p in pools}
    pool_names = {p[0].lower(): p[1] for p in pools}

    progress_fn(f"🏊 发现 {len(pools)} 个 LP: {', '.join(pool_names.values())}")

    # 拉取转账
    progress_fn("📡 拉取链上转账...")
    decimals = info["decimals"]
    records = load_transfers(contract, progress_fn, decimals, skip_sync=skip_sync)
    if not records:
        return {"error": "无转账记录"}

    # 价格历史（使用 price_cache 统一缓存层）
    progress_fn("📈 获取价格历史...")
    from price_cache import fetch_and_cache_history, get_price_at_block, get_or_compute_block_ts, get_cached_price_count, load_all_prices

    # 批量拉取并缓存历史价格
    cached_count = get_cached_price_count(contract)
    if cached_count < 100:
        fetch_and_cache_history(contract, pair, days=90,
                                rpcs=RPC_CANDIDATES,
                                token_decimals=info.get("decimals", 18),
                                records=records)
    else:
        # 增量更新（只拉最近的小时线）
        from price_cache import fetch_gecko_ohlcv
        import sqlite3 as _sqlite3
        recent = fetch_gecko_ohlcv(pair, "hour", 200)
        if recent:
            _pc_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "price_cache.db")
            _conn = _sqlite3.connect(_pc_db)
            _conn.executemany("INSERT OR REPLACE INTO hourly_price (contract, hour_ts, price, source) VALUES (?,?,?,?)",
                             [(contract, ts, p, "gecko") for ts, p in recent.items()])
            _conn.commit()
            _conn.close()

    slope, intercept = get_or_compute_block_ts(contract, records, RPC_CANDIDATES)

    # 预加载所有价格到内存（避免 smart_money 等模块频繁查 SQLite）
    _cached_prices = load_all_prices(contract)
    # 兼容：合并 GeckoTerminal 内存 price_map
    price_map = get_price_history(pair)
    _cached_prices.update({k: v for k, v in price_map.items() if k not in _cached_prices})
    print(f"[scan] 价格缓存: {len(_cached_prices)} 条已加载到内存", file=sys.stderr)

    def block_to_price(block):
        ts = slope * block + intercept
        hour_ts = int(ts // 3600) * 3600
        # 内存查找（O(1)）
        if hour_ts in _cached_prices:
            return _cached_prices[hour_ts]
        for delta in range(1, 5):
            if hour_ts + delta * 3600 in _cached_prices:
                return _cached_prices[hour_ts + delta * 3600]
            if hour_ts - delta * 3600 in _cached_prices:
                return _cached_prices[hour_ts - delta * 3600]
        return tp

    exclude = {ZERO, DEAD} | pool_set

    # 识别庄家（传入所有 pool）
    progress_fn("🔍 识别庄家...")
    whale_addrs = identify_whales(records, pools, total_supply)
    whale_set = set(whale_addrs)

    # LP 操纵检测
    progress_fn("🏊 LP 安全检测...")
    from lp_detect import get_lp_events, analyze_lp_manipulation
    lp_events = get_lp_events(pair, ALCHEMY_KEY, max_pages=3)
    lp_analysis = analyze_lp_manipulation(lp_events)

    # 全量 balance
    balances = defaultdict(float)
    # 同时构建 addr→records 倒排索引，避免后续 O(W×N) 遍历
    addr_records = defaultdict(list)
    for i, (block, fa, ta, amount) in enumerate(records):
        balances[fa] -= amount
        balances[ta] += amount
        addr_records[fa].append(i)
        addr_records[ta].append(i)

    # 庄家成本分析（使用倒排索引，O(N) 而非 O(W×N)）
    whale_results = []
    for addr in whale_addrs:
        total_buy_cost = 0
        total_buy_amount = 0
        total_sell_revenue = 0
        total_sell_amount = 0
        buy_cnt = 0
        sell_cnt = 0
        seen = set()  # 避免同一条记录被 fa/ta 重复处理
        for i in addr_records.get(addr, []):
            if i in seen:
                continue
            seen.add(i)
            block, fa, ta, amount = records[i]
            if fa in pool_set and ta == addr:
                price = block_to_price(block)
                total_buy_cost += amount * price
                total_buy_amount += amount
                buy_cnt += 1
            elif fa == addr and ta in pool_set:
                price = block_to_price(block)
                total_sell_revenue += amount * price
                total_sell_amount += amount
                sell_cnt += 1
        balance = max(balances.get(addr, 0), 0)
        avg_buy = total_buy_cost / total_buy_amount if total_buy_amount > 0 else 0
        avg_sell = total_sell_revenue / total_sell_amount if total_sell_amount > 0 else 0
        realized = total_sell_revenue - (total_sell_amount * avg_buy) if total_sell_amount > 0 and avg_buy > 0 else 0
        unrealized = balance * (tp - avg_buy) if balance > 0 and avg_buy > 0 else 0
        tag = ""
        if balance == 0 and total_buy_amount > 0:
            tag = "已清仓"
        elif total_sell_amount > total_buy_amount * 0.8:
            tag = "⚠️出货中"
        elif total_sell_amount == 0 and total_buy_amount > 0:
            tag = "💎未卖"
        whale_results.append({
            "addr": addr, "balance": balance,
            "buy_cnt": buy_cnt, "sell_cnt": sell_cnt,
            "total_buy_amount": total_buy_amount, "total_sell_amount": total_sell_amount,
            "total_buy_cost": total_buy_cost, "total_sell_revenue": total_sell_revenue,
            "avg_buy": avg_buy, "avg_sell": avg_sell,
            "realized": realized, "unrealized": unrealized,
            "total_pnl": realized + unrealized, "tag": tag,
        })
    whale_results.sort(key=lambda x: -x["total_buy_amount"])

    # 抛压（只算真人庄家，排除合约地址）
    whale_remaining = sum(max(balances.get(a, 0), 0) for a in whale_set)
    # LP 池深度估算：使用链上 balanceOf 查询（更准确）
    lp_est = 0
    for p in pool_set:
        bal = balances.get(p, 0)
        if bal < 0:
            # LP 合约在 transfer_graph 中 balance 为负说明是"池子给出去的"
            # 使用绝对值作为池子实际持有量的近似（从交易记录推导）
            lp_est += abs(bal)
        elif bal > 0:
            lp_est += bal
    lp_est = max(lp_est, total_supply * 0.01)
    impact = whale_remaining / (lp_est + whale_remaining) * 100
    # 防止超过 100%（数据不完整时）
    pct_supply = min(whale_remaining / total_supply * 100, 100) if total_supply > 0 else 0

    pressure = {
        "remaining": whale_remaining,
        "remaining_usd": whale_remaining * tp,
        "pct_supply": pct_supply,
        "impact_pct": round(impact, 1),  # 保留原始值，不人为截断
    }

    # 持仓集中度 — 阈值改为基于 USD 价值（>$100 算大户）
    min_hold_value = 100 / tp if tp > 0 else 1000
    min_hold_value = max(min_hold_value, 100)  # 至少 100 token
    all_holders = {k: v for k, v in balances.items() if v > min_hold_value and k not in exclude}
    sorted_all = sorted(all_holders.items(), key=lambda x: -x[1])

    # Top 50 合约检查并行化
    contract_addrs = set()
    top50 = sorted_all[:50]
    if top50:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        def _check_contract(addr):
            return addr, is_contract_address(addr)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_check_contract, addr): addr for addr, _ in top50}
            for future in as_completed(futures):
                addr, is_contract = future.result()
                if is_contract:
                    contract_addrs.add(addr)
                    print(f"  排除合约持仓: {addr[:10]}..{addr[-4:]} ({all_holders[addr]:,.0f})", file=sys.stderr)

    real_holders = {k: v for k, v in all_holders.items() if k not in contract_addrs}
    actual_supply = sum(v for v in real_holders.values())
    denom = actual_supply if actual_supply > 0 else total_supply
    sorted_h = sorted(real_holders.values(), reverse=True)

    concentration = {
        "top5_pct": sum(sorted_h[:5]) / denom * 100 if denom > 0 else 0,
        "top10_pct": sum(sorted_h[:10]) / denom * 100 if denom > 0 else 0,
        "top20_pct": sum(sorted_h[:20]) / denom * 100 if denom > 0 else 0,
    }

    # Top 20 持仓
    sorted_holders = sorted(real_holders.items(), key=lambda x: -x[1])
    top_holders = []
    for i, (addr, bal) in enumerate(sorted_holders[:20], 1):
        pct = bal / denom * 100 if denom > 0 else 0
        top_holders.append({
            "rank": i, "addr": addr, "balance": bal,
            "pct": pct, "usd": bal * tp,
            "is_whale": addr in whale_set,
        })

    # 散户分析（排除所有 pool 地址）
    # 散户：持仓 > 动态阈值，排除庄家和排除地址
    retail_threshold = 10 / tp if tp > 0 else 100
    retail_threshold = max(retail_threshold, 10)  # 至少 10 token
    retail = {k: v for k, v in balances.items() if v > retail_threshold and k not in whale_set and k not in exclude}
    holdings = sorted(retail.values())
    total_retail = len(holdings)
    retail_stats = {"count": total_retail, "avg_usd": 0, "median_usd": 0, "distribution": {}}
    if total_retail > 0:
        avg_hold = sum(holdings) / total_retail
        median_hold = holdings[total_retail // 2]
        retail_stats["avg_usd"] = avg_hold * tp
        retail_stats["median_usd"] = median_hold * tp
        buckets = {"<$100": 0, "$100-1k": 0, "$1k-10k": 0, "$10k-100k": 0, ">$100k": 0}
        for h in holdings:
            usd = h * tp
            if usd < 100: buckets["<$100"] += 1
            elif usd < 1000: buckets["$100-1k"] += 1
            elif usd < 10000: buckets["$1k-10k"] += 1
            elif usd < 100000: buckets["$10k-100k"] += 1
            else: buckets[">$100k"] += 1
        retail_stats["distribution"] = buckets

    # 分仓检测
    progress_fn("🔗 分仓检测...")
    from shard_detect import detect_shards
    shard_results = detect_shards(records, whale_addrs, pools, total_supply)

    # 庄家关联聚类
    progress_fn("🕸️ 庄家关联分析...")
    from cluster import cluster_whales
    clusters = cluster_whales(records, whale_addrs, shard_results, pools)

    # 庄家标签
    progress_fn("🏷️ 庄家标签...")
    from labeler import label_whales
    whale_labels = label_whales(records, whale_addrs, pools, total_supply, shard_results, clusters)

    # 跨合约追踪（仅 deep 模式）
    cross_track_results = {}
    fund_trace_results = {}
    if deep:
        progress_fn("🔎 跨合约追踪（耗时较长）...")
        from cross_track import cross_track_whales
        cross_track_results = cross_track_whales(whale_addrs, contract, max_whales=5, max_pages=2)

        progress_fn("💸 资金溯源...")
        from fund_trace import trace_whale_funds
        fund_trace_results = trace_whale_funds(whale_addrs, ALCHEMY_KEY, max_whales=5)

        progress_fn("🧠 聪明钱识别...")
        from smart_money import find_smart_money, track_smart_money_activity
        smart_money_list = find_smart_money(records, pools, total_supply, tp, block_to_price, whale_addrs)
        smart_money_activity = {}
        if smart_money_list:
            smart_addrs = [sm["addr"] for sm in smart_money_list[:5]]
            smart_money_activity = track_smart_money_activity(smart_addrs, ALCHEMY_KEY, max_addrs=3)

    # 风险评估
    risks = []
    if info["owner"] != "0x" + "0" * 40:
        risks.append("Owner 未放弃")
    if sorted_h and sum(sorted_h[:5]) / denom > 0.5:
        risks.append(f"Top5 持仓 {sum(sorted_h[:5])/denom*100:.0f}%，高度集中")
    whale_sell_total = sum(w["total_sell_revenue"] for w in whale_results)
    whale_buy_total = sum(w["total_buy_cost"] for w in whale_results)
    if whale_buy_total > 0 and whale_sell_total > whale_buy_total * 1.2:
        risks.append(f"庄家净出货（卖/买={whale_sell_total/whale_buy_total:.2f}）")
    # 分仓风险
    shard_dumpers = [w for w, d in shard_results.items() if "出货" in d["pattern"]]
    if shard_dumpers:
        risks.append(f"{len(shard_dumpers)}个庄家通过分仓出货")
    # 关联风险
    big_clusters = [c for c in clusters if c["size"] > 2]
    if big_clusters:
        risks.append(f"发现{len(big_clusters)}个庄家团伙（最大{big_clusters[0]['size']}个地址）")
    # LP 风险
    if lp_analysis.get("patterns"):
        for p in lp_analysis["patterns"]:
            if "\U0001f534" in p or "\u26a0" in p or "🔴" in p or "⚠️" in p:
                risks.append(p)

    result = {
        "info": info,
        "token_price": tp,
        "dex_data": dex_data,
        "bnb_price": bnb_price,
        "total_records": len(records),
        "total_holders": len(real_holders),
        "whale_addrs": whale_addrs,
        "whale_results": whale_results,
        "pressure": pressure,
        "concentration": concentration,
        "top_holders": top_holders,
        "retail": retail_stats,
        "risks": risks,
        "pools": pools,
        "shard_results": shard_results,
        "clusters": clusters,
        "whale_labels": whale_labels,
        "cross_track": cross_track_results,
        "fund_trace": fund_trace_results,
        "smart_money": smart_money_list if deep else [],
        "smart_money_activity": smart_money_activity if deep else {},
        "lp_analysis": lp_analysis,
    }

    # 持仓快照（持久化到 DB，可用于对比变化）
    progress_fn("📸 生成持仓快照...")
    try:
        from snapshot import take_whale_snapshot
        snap = take_whale_snapshot(contract, whale_addrs[:10], tp)
        if snap:
            result["snapshot"] = snap
    except Exception as e:
        print(f"  快照生成失败（非关键）: {e}", file=sys.stderr)

    # 风险评分（需要完整 result）
    from risk_score import calculate_risk_score
    result["risk_score"] = calculate_risk_score(result)

    return result
