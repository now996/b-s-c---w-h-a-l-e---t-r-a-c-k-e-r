#!/usr/bin/env python3
"""
data_source.py — 统一数据源抽象层
封装 Alchemy / DexScreener / GeckoTerminal / 公共 RPC 的所有外部 API 调用
消除 scan_core.py 和 monitor.py 之间的代码重复
"""
import os
import sys
import json
import time
import threading
import requests
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")

ZERO = "0x0000000000000000000000000000000000000000"
DEAD = "0x000000000000000000000000000000000000dead"
WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

# BSC DEX 路由器合约（买卖不一定直接经过 LP，可能经过路由器中转）
# 注意：此列表不完整，scan_core 会自动从 DB 中发现与 LP 高频交互的合约
DEX_ROUTERS = {
    "0x10ed43c718714eb63d5aa57b78b54704e256024e",  # PancakeSwap V2 Router
    "0x1b81d678ffb9c0263b24a97847620c99d213eb14",  # PancakeSwap V3 SwapRouter
    "0x13f4ea83d0bd40e75c8222255bc855a974568dd4",  # PancakeSwap SmartRouter
    "0xb300000b72deaeb607a12d5f54773d1c19c7028d",  # PancakeSwap SmartRouter V2 (实际地址)
    "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",  # PancakeSwap V3 NonfungiblePositionManager
    "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",  # PancakeSwap V3 PositionManager (实际地址)
    "0x111111125421c6f28a6c27c06ee7c2bc3c2713f6",  # 1inch Router
    "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df",  # PancakeSwap V3 管理/路由合约
    "0x2480faeb931272cd1f7375d8f4c104a4db5fff63",  # PancakeSwap 路由合约
    "0x00000000214b106a4d67113a969ab6e7a56cfb0d",  # DEX 聚合器
}

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


def _load_config():
    """加载 config.json"""
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH) as f:
                return json.load(f)
    except Exception as e:
        print(f"[data_source] 读取 config 失败: {e}", file=sys.stderr)
    return {}


def _get_rpc_candidates():
    """从 config + 环境变量 + 默认列表构建 RPC 候选列表"""
    config = _load_config()
    rpcs = []

    # config.json 中的自定义 RPC
    for item in config.get("bsc_rpcs", []):
        if isinstance(item, str) and item and item not in rpcs:
            rpcs.append(item)
    primary = config.get("bsc_rpc")
    if isinstance(primary, str) and primary and primary not in rpcs:
        rpcs.append(primary)

    # 环境变量
    env_rpc = os.environ.get("BSC_RPC")
    if env_rpc and env_rpc not in rpcs:
        rpcs.append(env_rpc)

    # 默认列表
    for item in DEFAULT_RPCS:
        if item not in rpcs:
            rpcs.append(item)

    return rpcs


# ═══════════════════════════════════════════
# AlchemyClient
# ═══════════════════════════════════════════

class AlchemyClient:
    """封装所有 Alchemy BSC API 调用"""

    def __init__(self, api_key=None):
        if api_key:
            self.api_key = api_key
        else:
            config = _load_config()
            self.api_key = config.get("alchemy_key", "") or os.environ.get("ALCHEMY_KEY", "")
        self.base_url = f"https://bnb-mainnet.g.alchemy.com/v2/{self.api_key}"

    @property
    def available(self):
        return bool(self.api_key)

    def _post(self, payload, timeout=30):
        """发送 JSON-RPC 请求到 Alchemy"""
        for retry in range(3):
            try:
                r = requests.post(self.base_url, json=payload, timeout=timeout)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(f"Alchemy error: {data['error']}")
                return data.get("result")
            except requests.exceptions.Timeout:
                print(f"[alchemy] 请求超时，重试 {retry+1}/3", file=sys.stderr)
                time.sleep(2)
            except Exception as e:
                if retry < 2:
                    time.sleep(2)
                else:
                    raise
        return None

    def get_asset_transfers(self, contract, from_block="0x0", to_block="latest",
                            max_count=1000, page_key=None, order="asc",
                            category=None):
        """
        调用 alchemy_getAssetTransfers 获取 ERC20 转账记录（分页）

        返回: (transfers_list, next_page_key_or_None)
        """
        params = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "contractAddresses": [contract.lower()],
            "category": category or ["erc20"],
            "maxCount": hex(max_count),
            "order": order,
            "withMetadata": True,
        }
        if page_key:
            params["pageKey"] = page_key

        payload = {
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [params],
            "id": 1
        }

        result = self._post(payload, timeout=30)
        if not result:
            return [], None

        transfers = result.get("transfers", [])
        next_key = result.get("pageKey")
        return transfers, next_key

    def get_token_balances(self, address, contracts=None):
        """
        调用 alchemy_getTokenBalances 获取地址的 token 余额

        address: 钱包地址
        contracts: 可选，指定要查询的 token 合约列表（最多 100）

        返回: {contract_address: balance_wei_str, ...}
        """
        if contracts:
            payload = {
                "jsonrpc": "2.0",
                "method": "alchemy_getTokenBalances",
                "params": [address, contracts],
                "id": 2
            }
        else:
            payload = {
                "jsonrpc": "2.0",
                "method": "alchemy_getTokenBalances",
                "params": [address, "erc20"],
                "id": 2
            }

        result = self._post(payload, timeout=15)
        if not result:
            return {}

        balances = {}
        for item in result.get("tokenBalances", []):
            addr = item.get("contractAddress", "").lower()
            bal = item.get("tokenBalance", "0x0")
            error = item.get("error")
            if error or not bal or bal == "0x" or bal == "0x0":
                continue
            try:
                balances[addr] = int(bal, 16)
            except (ValueError, TypeError):
                continue
        return balances

    def get_block_number(self):
        """获取最新区块号"""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }
        result = self._post(payload, timeout=10)
        if result:
            return int(result, 16)
        return 0


# ═══════════════════════════════════════════
# DexScreenerClient
# ═══════════════════════════════════════════

class DexScreenerClient:
    """封装 DexScreener API"""

    BASE_URL = "https://api.dexscreener.com"

    def get_token_price(self, contract):
        """
        查询代币价格和 DEX 数据
        返回: (price_usd, dex_data_dict)
        """
        for retry in range(3):
            try:
                url = f"{self.BASE_URL}/latest/dex/tokens/{contract}"
                r = requests.get(url, timeout=10)
                if r.status_code == 429:
                    wait = min(2 ** retry, 10)
                    print(f"[dexscreener] 429 限速，等待 {wait}s", file=sys.stderr)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                pairs = r.json().get("pairs", [])
                if pairs:
                    return float(pairs[0].get("priceUsd") or 0), pairs[0]
                break  # 无数据，不重试
            except Exception as e:
                if retry < 2:
                    time.sleep(2 ** retry)
                else:
                    print(f"[dexscreener] 查询失败: {e}", file=sys.stderr)
        return 0, {}

    def get_pair_info(self, pair_addr):
        """查询交易对详情"""
        try:
            url = f"{self.BASE_URL}/latest/dex/pairs/bsc/{pair_addr}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            pairs = r.json().get("pairs", [])
            if pairs:
                return pairs[0]
        except Exception as e:
            print(f"[dexscreener] 查询 pair 失败: {e}", file=sys.stderr)
        return {}


# ═══════════════════════════════════════════
# GeckoTerminalClient
# ═══════════════════════════════════════════

class GeckoTerminalClient:
    """封装 GeckoTerminal API"""

    BASE_URL = "https://api.geckoterminal.com/api/v2"

    def get_ohlcv(self, pair_addr, timeframe="hour", limit=1000):
        """获取 K 线数据"""
        try:
            url = f"{self.BASE_URL}/networks/bsc/pools/{pair_addr}/ohlcv/{timeframe}?limit={limit}"
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                ohlcv = r.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                return ohlcv
        except Exception as e:
            print(f"[gecko] OHLCV 查询失败: {e}", file=sys.stderr)
        return []

    def get_pool_info(self, pair_addr):
        """获取交易池信息"""
        try:
            url = f"{self.BASE_URL}/networks/bsc/pools/{pair_addr}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.json().get("data", {}).get("attributes", {})
        except Exception as e:
            print(f"[gecko] pool 查询失败: {e}", file=sys.stderr)
        return {}


# ═══════════════════════════════════════════
# RPCClient
# ═══════════════════════════════════════════

class RPCClient:
    """统一公共 RPC 调用，带轮询重试、粘性 RPC 和线程安全"""

    def __init__(self, rpcs=None):
        self.rpcs = rpcs or _get_rpc_candidates()
        self._sticky_idx = 0  # 粘性 RPC：记住上次成功的索引
        self._lock = __import__('threading').Lock()

    def call(self, method, params, timeout=10):
        """通用 RPC 调用，优先使用上次成功的 RPC（粘性），失败后轮询"""
        last_errors = []
        # 先尝试粘性 RPC
        order = list(range(len(self.rpcs)))
        if self._sticky_idx < len(order):
            order.insert(0, order.pop(self._sticky_idx))

        for idx in order:
            rpc = self.rpcs[idx] if idx < len(self.rpcs) else None
            if not rpc:
                continue
            try:
                r = requests.post(rpc, json={
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": 1
                }, timeout=timeout)
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(str(data["error"]))
                # 更新粘性索引
                with self._lock:
                    self._sticky_idx = idx
                return data.get("result"), rpc
            except Exception as e:
                last_errors.append(f"{rpc}: {e}")
                continue
        raise RuntimeError(f"all rpc failed: {'; '.join(last_errors[-3:])}")

    def eth_call(self, to, data):
        """调用合约的 view 函数"""
        try:
            result, _ = self.call("eth_call", [{"to": to, "data": data}, "latest"], timeout=10)
            return result or "0x"
        except Exception:
            return "0x"

    def get_latest_block(self):
        """获取最新区块号"""
        try:
            result, _ = self.call("eth_blockNumber", [], timeout=10)
            return int(result, 16) if result else 0
        except Exception:
            return 0

    def get_logs(self, address, from_block, to_block, topic=None):
        """
        调用 eth_getLogs 获取事件日志
        address: 合约地址
        from_block/to_block: 十进制区块号
        topic: 可选的事件 topic
        """
        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": address.lower(),
        }]
        if topic:
            params[0]["topics"] = [topic]

        try:
            result, _ = self.call("eth_getLogs", params, timeout=30)
            return result if isinstance(result, list) else []
        except Exception:
            return []

    def get_code(self, address):
        """获取地址的合约代码，用于判断是否是合约"""
        try:
            result, _ = self.call("eth_getCode", [address, "latest"], timeout=5)
            return result or "0x"
        except Exception:
            return "0x"

    def is_contract(self, address):
        """判断地址是否是合约"""
        code = self.get_code(address)
        return len(code) > 4

    def get_block_timestamp(self, block_number):
        """获取区块时间戳"""
        try:
            result, _ = self.call("eth_getBlockByNumber", [hex(block_number), False], timeout=10)
            ts_hex = (result or {}).get("timestamp")
            if ts_hex:
                return int(ts_hex, 16)
        except Exception:
            pass
        return 0


# ═══════════════════════════════════════════
# PriceProvider
# ═══════════════════════════════════════════

class PriceProvider:
    """统一价格提供者，支持多源降级"""

    def __init__(self):
        self._dex = DexScreenerClient()
        self._gecko = GeckoTerminalClient()
        self._rpc = RPCClient()
        self._bnb_price_cache = {"price": 0, "ts": 0}
        self._token_price_cache = {}  # contract -> {"price": x, "dex_data": {}, "ts": float}
        self._config_cache = None  # 缓存 config 避免重复读文件
        self._config_cache_ts = 0

    def get_bnb_price(self):
        """
        获取 BNB 价格（降级链: Binance → DexScreener → 硬编码）
        """
        # 缓存 60 秒
        now = time.time()
        if self._bnb_price_cache["price"] > 0 and now - self._bnb_price_cache["ts"] < 60:
            return self._bnb_price_cache["price"]

        # 源1: Binance API
        try:
            r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT", timeout=5)
            price = float(r.json()["price"])
            self._bnb_price_cache = {"price": price, "ts": now}
            return price
        except Exception:
            pass

        # 源2: DexScreener WBNB pair
        try:
            price, _ = self._dex.get_token_price(WBNB)
            if price > 0:
                self._bnb_price_cache = {"price": price, "ts": now}
                return price
        except Exception:
            pass

        # 降级: 使用上次缓存的价格，否则 600
        fallback = self._bnb_price_cache.get("price", 600)
        if fallback > 0 and fallback != 600:
            print(f"[price] BNB 价格获取失败，使用缓存值 {fallback}", file=sys.stderr)
            return fallback
        return 600

    def get_token_price(self, contract):
        """
        获取代币价格（降级链: DexScreener → GeckoTerminal → 返回 0）
        返回: (price_usd, dex_data_dict)
        """
        # 缓存 30 秒
        now = time.time()
        cached = self._token_price_cache.get(contract)
        if cached and now - cached["ts"] < 30:
            return cached["price"], cached["dex_data"]

        # 源1: DexScreener
        try:
            price, dex_data = self._dex.get_token_price(contract)
            if price > 0:
                self._token_price_cache[contract] = {"price": price, "dex_data": dex_data, "ts": now}
                return price, dex_data
        except Exception:
            pass

        # 源2: GeckoTerminal（需要 pair 地址，从缓存 config 获取）
        now_cfg = time.time()
        if self._config_cache is None or now_cfg - self._config_cache_ts > 60:
            self._config_cache = _load_config()
            self._config_cache_ts = now_cfg
        config = self._config_cache
        contract_cfg = config.get("contracts", {}).get(contract.lower(), {})
        pair = contract_cfg.get("pair", "")
        if pair:
            try:
                pool_info = self._gecko.get_pool_info(pair)
                base_price = pool_info.get("base_token_price_usd")
                if base_price and float(base_price) > 0:
                    price = float(base_price)
                    dex_data = {"priceUsd": str(price)}
                    self._token_price_cache[contract] = {"price": price, "dex_data": dex_data, "ts": now}
                    return price, dex_data
            except Exception:
                pass

        # 降级: 返回 0
        return 0, {}


# ═══════════════════════════════════════════
# 便捷实例（懒加载）
# ═══════════════════════════════════════════

_alchemy_client = None
_rpc_client = None
_price_provider = None
_singleton_lock = threading.Lock()


def get_alchemy_client():
    global _alchemy_client
    if _alchemy_client is None:
        with _singleton_lock:
            if _alchemy_client is None:
                _alchemy_client = AlchemyClient()
    return _alchemy_client


def get_rpc_client():
    global _rpc_client
    if _rpc_client is None:
        with _singleton_lock:
            if _rpc_client is None:
                _rpc_client = RPCClient()
    return _rpc_client


def get_price_provider():
    global _price_provider
    if _price_provider is None:
        with _singleton_lock:
            if _price_provider is None:
                _price_provider = PriceProvider()
    return _price_provider
