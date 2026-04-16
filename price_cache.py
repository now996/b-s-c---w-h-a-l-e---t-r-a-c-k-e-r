"""
price_cache.py — 统一价格缓存层
多源聚合：SQLite 缓存 → GeckoTerminal OHLCV → 链上 LP Reserves 回溯 → DexScreener 当前价格
"""
import os
import sys
import time
import sqlite3
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "data", "price_cache.db")

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


def _get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS hourly_price (
        contract TEXT, hour_ts INTEGER, price REAL, source TEXT,
        PRIMARY KEY (contract, hour_ts)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS block_ts (
        contract TEXT PRIMARY KEY, slope REAL, intercept REAL, updated_at REAL
    )""")
    conn.commit()
    return conn


def _rpc_call(rpcs, method, params, timeout=10):
    for rpc in rpcs:
        try:
            r = requests.post(rpc, json={
                "jsonrpc": "2.0", "method": method,
                "params": params, "id": 1
            }, timeout=timeout)
            data = r.json()
            if "error" not in data:
                return data.get("result")
        except Exception:
            continue
    return None


# ═══════════════════════════════════════════
# 价格源 1: GeckoTerminal 历史 OHLCV
# ═══════════════════════════════════════════

def fetch_gecko_ohlcv(pair_addr, timeframe="hour", limit=1000):
    """从 GeckoTerminal 拉取历史 OHLCV，返回 {hour_ts: price}"""
    try:
        url = f"https://api.geckoterminal.com/api/v2/networks/bsc/pools/{pair_addr}/ohlcv/{timeframe}?limit={limit}"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return {}
        ohlcv = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        result = {}
        for c in ohlcv:
            ts = int(c[0])
            vwap = (float(c[1]) + float(c[2]) + float(c[3]) + float(c[4])) / 4
            result[ts] = vwap
        return result
    except Exception:
        return {}


def fetch_and_cache_history(contract, pair_addr, days=90, rpcs=None, token_decimals=18, records=None):
    """批量拉取历史价格并存入 SQLite 缓存（GeckoTerminal + LP Reserves 回溯）"""
    contract = contract.lower()
    conn = _get_db()
    try:
        all_prices = {}

        # 1. 小时线分页拉取（每页 1000 条，最多 5 页 ≈ 208 天）
        before_ts = None
        for page in range(5):
            url = f"https://api.geckoterminal.com/api/v2/networks/bsc/pools/{pair_addr}/ohlcv/hour?limit=1000"
            if before_ts:
                url += f"&before_timestamp={before_ts}"
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    ohlcv = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                    if not ohlcv:
                        break
                    for c in ohlcv:
                        ts = int(c[0])
                        vwap = (float(c[1]) + float(c[2]) + float(c[3]) + float(c[4])) / 4
                        if vwap > 0:
                            all_prices[ts] = vwap
                    before_ts = int(ohlcv[-1][0]) - 1
                else:
                    break
            except Exception:
                break
            time.sleep(1.2)

        # 2. 日线补充
        try:
            url = f"https://api.geckoterminal.com/api/v2/networks/bsc/pools/{pair_addr}/ohlcv/day?limit=1000"
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                ohlcv = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                for c in ohlcv:
                    ts = int(c[0])
                    vwap = (float(c[1]) + float(c[2]) + float(c[3]) + float(c[4])) / 4
                    for h in range(24):
                        h_ts = ts + h * 3600
                        if h_ts not in all_prices and vwap > 0:
                            all_prices[h_ts] = vwap
        except Exception:
            pass

        gecko_count = len(all_prices)

        # 3. LP Reserves 回溯补充（覆盖 GeckoTerminal 之前的时间段）
        if rpcs and records and all_prices:
            try:
                earliest_gecko_ts = min(all_prices.keys())
                # 获取 BNB 价格用于 USD 换算
                bnb_usd = _get_bnb_price()

                # 从 records 中采样 GeckoTerminal 未覆盖的区块
                # 计算 block→ts 映射
                slope_row = conn.execute("SELECT slope, intercept FROM block_ts WHERE contract=?",
                                        (contract,)).fetchone()
                if slope_row and slope_row[0] > 0:
                    slope, intercept = slope_row
                    # 每 4 小时采样一个点
                    sample_interval = 4 * 3600 / slope  # 4 小时对应多少个 block
                    min_block = records[0][0]
                    max_block = records[-1][0]

                    # 找到 GeckoTerminal 最早数据对应的 block
                    gecko_start_block = int((earliest_gecko_ts - intercept) / slope)
                    sample_block = min_block
                    lp_count = 0
                    # 优先用 Alchemy（支持 archive 节点查询历史 block）
                    alchemy_key = os.environ.get("ALCHEMY_KEY", "")
                    lp_rpcs = rpcs[:]
                    if alchemy_key:
                        alchemy_url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
                        lp_rpcs = [alchemy_url] + lp_rpcs
                    while sample_block < gecko_start_block and lp_count < 200:
                        block_hex = hex(int(sample_block))
                        bnb_price_per_token = estimate_price_from_reserves(
                            pair_addr, block_hex, lp_rpcs, token_decimals
                        )
                        if bnb_price_per_token and bnb_price_per_token > 0:
                            usd_price = bnb_price_per_token * bnb_usd
                            ts = slope * sample_block + intercept
                            hour_ts = int(ts // 3600) * 3600
                            if hour_ts not in all_prices:
                                all_prices[hour_ts] = usd_price
                                lp_count += 1
                        sample_block += sample_interval
                        time.sleep(0.1)

                    if lp_count > 0:
                        print(f"[price_cache] LP Reserves 回溯补充 {lp_count} 条 ({contract[:10]}...)", file=sys.stderr)
            except Exception as e:
                print(f"[price_cache] LP Reserves 回溯失败: {e}", file=sys.stderr)

        if not all_prices:
            return 0

        # 批量写入
        batch = [(contract, ts, price, "gecko" if ts >= min(all_prices.keys()) else "lp_reserves")
                 for ts, price in all_prices.items()]
        conn.executemany(
            "INSERT OR REPLACE INTO hourly_price (contract, hour_ts, price, source) VALUES (?,?,?,?)",
            batch
        )
        conn.commit()
        lp_extra = len(all_prices) - gecko_count
        print(f"[price_cache] 缓存 {len(batch)} 条价格 (gecko={gecko_count} lp={lp_extra}) ({contract[:10]}...)", file=sys.stderr)
        return len(batch)
    finally:
        conn.close()


def _get_bnb_price():
    """获取当前 BNB/USD 价格"""
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=binancecoin&vs_currencies=usd", timeout=10)
        return r.json().get("binancecoin", {}).get("usd", 600)
    except Exception:
        return 600


# ═══════════════════════════════════════════
# 价格源 2: 链上 LP Reserves 回溯
# ═══════════════════════════════════════════

_token0_cache = {}


def _get_token0(pair, rpcs):
    if pair in _token0_cache:
        return _token0_cache[pair]
    result = _rpc_call(rpcs, "eth_call", [{"to": pair, "data": "0x0dfe1681"}, "latest"])
    if result and len(result) >= 42:
        token0 = "0x" + result[-40:]
        _token0_cache[pair] = token0
        return token0
    return None


def estimate_price_from_reserves(pair, block_hex, rpcs, token_decimals=18):
    """通过历史区块的 LP Reserves 估算价格（需要 archive 节点）"""
    try:
        result = _rpc_call(rpcs, "eth_call",
                          [{"to": pair, "data": "0x0902f1ac"}, block_hex],
                          timeout=15)
        if not result or len(result) < 130:
            return None
        r0 = int(result[2:66], 16)
        r1 = int(result[66:130], 16)
        token0 = _get_token0(pair, rpcs)
        if token0 and token0.lower() == WBNB:
            bnb_reserve = r0 / 1e18
            token_reserve = r1 / (10 ** token_decimals)
        else:
            bnb_reserve = r1 / 1e18
            token_reserve = r0 / (10 ** token_decimals)
        if token_reserve > 0 and bnb_reserve > 0:
            # 返回 token 的 BNB 价格（还需乘以 BNB/USD）
            return bnb_reserve / token_reserve
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════
# 价格源 3: DexScreener 当前价格
# ═══════════════════════════════════════════

_dex_cache = {}
_DEX_CACHE_TTL = 60


def _get_dex_price(contract):
    now = time.time()
    cached = _dex_cache.get(contract)
    if cached and now - cached[1] < _DEX_CACHE_TTL:
        return cached[0]
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{contract}", timeout=10)
        pairs = r.json().get("pairs", [])
        if pairs:
            price = float(pairs[0].get("priceUsd") or 0)
            _dex_cache[contract] = (price, now)
            return price
    except Exception:
        pass
    _dex_cache[contract] = (0, now)
    return 0


# ═══════════════════════════════════════════
# Block → Timestamp 映射
# ═══════════════════════════════════════════

def get_or_compute_block_ts(contract, records, rpcs=None):
    """获取或计算 block-to-timestamp 线性回归参数"""
    contract = contract.lower()
    conn = _get_db()
    try:
        row = conn.execute("SELECT slope, intercept FROM block_ts WHERE contract=?", (contract,)).fetchone()
        if row and row[0] > 0:
            return row[0], row[1]
    finally:
        conn.close()

    if not records:
        return 0.451, 0

    rpcs = rpcs or DEFAULT_RPCS
    blocks = [records[0][0], records[len(records)//2][0], records[-1][0]]
    block_ts_map = {}
    for b in blocks:
        result = _rpc_call(rpcs, "eth_getBlockByNumber", [hex(b), False])
        if result:
            ts_hex = result.get("timestamp")
            if ts_hex:
                block_ts_map[b] = int(ts_hex, 16)

    if len(block_ts_map) < 2:
        return 0.451, 0

    items = sorted(block_ts_map.items())
    xs = [b for b, _ in items]
    ys = [t for _, t in items]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x)**2 for x in xs)
    if denom == 0:
        return 0.451, 0
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom
    intercept = mean_y - slope * mean_x

    # 缓存到 SQLite
    conn = _get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO block_ts (contract, slope, intercept, updated_at) VALUES (?,?,?,?)",
                     (contract, slope, intercept, time.time()))
        conn.commit()
    finally:
        conn.close()

    return slope, intercept


# ═══════════════════════════════════════════
# 统一入口：获取某区块时的价格
# ═══════════════════════════════════════════

def get_price_at_block(contract, block, slope, intercept, fallback_price=0):
    """
    获取某区块时的代币 USD 价格。
    查询优先级：SQLite 缓存 → fallback_price（当前价格）
    """
    contract = contract.lower()
    ts = slope * block + intercept
    hour_ts = int(ts // 3600) * 3600

    conn = _get_db()
    try:
        # 精确匹配小时
        row = conn.execute("SELECT price FROM hourly_price WHERE contract=? AND hour_ts=?",
                          (contract, hour_ts)).fetchone()
        if row:
            return row[0]

        # 前后 4 小时搜索
        for delta in range(1, 5):
            row = conn.execute("SELECT price FROM hourly_price WHERE contract=? AND hour_ts=?",
                              (contract, hour_ts + delta * 3600)).fetchone()
            if row:
                return row[0]
            row = conn.execute("SELECT price FROM hourly_price WHERE contract=? AND hour_ts=?",
                              (contract, hour_ts - delta * 3600)).fetchone()
            if row:
                return row[0]
    finally:
        conn.close()

    return fallback_price


def get_cached_price_count(contract):
    """查看缓存中有多少条价格记录"""
    conn = _get_db()
    try:
        row = conn.execute("SELECT COUNT(*) FROM hourly_price WHERE contract=?",
                          (contract.lower(),)).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def load_all_prices(contract):
    """一次性加载某合约所有缓存价格到内存 dict {hour_ts: price}"""
    conn = _get_db()
    try:
        rows = conn.execute("SELECT hour_ts, price FROM hourly_price WHERE contract=?",
                           (contract.lower(),)).fetchall()
        return {row[0]: row[1] for row in rows}
    finally:
        conn.close()
