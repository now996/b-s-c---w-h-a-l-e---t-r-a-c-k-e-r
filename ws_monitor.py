"""
ws_monitor.py — WebSocket 实时事件推送模块
通过 WSS 订阅 BSC 链上 Transfer logs，替代 HTTP 轮询
断线自动降级为 HTTP catch-up，重连后恢复 WSS
"""
import asyncio
import json
import sys
import time
import threading


def _build_wss_url(config):
    """从 config 构建 WSS URL"""
    endpoints = config.get("wss_endpoints", [])
    if endpoints:
        url = endpoints[0]
        # 替换 {key} 占位符
        alchemy_key = config.get("alchemy_key", "")
        if "{key}" in url and alchemy_key:
            url = url.replace("{key}", alchemy_key)
        return url
    # 默认用 Alchemy WSS
    alchemy_key = config.get("alchemy_key", "")
    if alchemy_key:
        return f"wss://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    return None


async def _ws_subscribe(wss_url, contracts, on_log, on_disconnect, stop_event):
    """
    WSS 订阅核心逻辑
    contracts: [contract_addr, ...]
    on_log: callback(log_dict) — 收到 log 时调用
    on_disconnect: callback(last_block) — 断线时调用
    """
    try:
        import websockets
    except ImportError:
        print("[ws] websockets 库未安装，请运行: pip install websockets", file=sys.stderr)
        on_disconnect(0)
        return

    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    last_block = 0
    reconnect_delay = 1

    while not stop_event.is_set():
        try:
            print(f"[ws] 连接 {wss_url[:50]}...", file=sys.stderr)
            async with websockets.connect(wss_url, ping_interval=20, ping_timeout=30) as ws:
                reconnect_delay = 1
                print("[ws] WSS 已连接", file=sys.stderr)

                # 订阅所有合约的 Transfer logs
                sub_params = {
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["logs", {
                        "address": [c.lower() for c in contracts],
                        "topics": [TRANSFER_TOPIC],
                    }],
                    "id": 1
                }
                await ws.send(json.dumps(sub_params))
                sub_resp = await asyncio.wait_for(ws.recv(), timeout=10)
                sub_data = json.loads(sub_resp)
                sub_id = sub_data.get("result")
                if not sub_id:
                    print(f"[ws] 订阅失败: {sub_data}", file=sys.stderr)
                    await asyncio.sleep(5)
                    continue
                print(f"[ws] 订阅成功 (id={sub_id}), 监控 {len(contracts)} 个合约", file=sys.stderr)
                # 通知连接已恢复，携带最后区块号供主循环补查
                on_log({"_ws_connected": True, "blockNumber": "0x0", "_reconnected": True, "_last_ws_block": last_block})

                # 持续接收 logs
                while not stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        # 60s 无消息，发 ping 检查连接
                        try:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=15)
                        except Exception:
                            print("[ws] ping 超时，重连", file=sys.stderr)
                            break
                        continue

                    data = json.loads(msg)
                    params = data.get("params")
                    if not params:
                        continue
                    log = params.get("result")
                    if not log or not isinstance(log, dict):
                        continue

                    block_hex = log.get("blockNumber", "0x0")
                    block = int(block_hex, 16) if block_hex else 0
                    if block > last_block:
                        last_block = block

                    try:
                        on_log(log)
                    except Exception as e:
                        print(f"[ws] on_log 错误: {e}", file=sys.stderr)

        except Exception as e:
            print(f"[ws] 连接断开: {e} | 重连 {reconnect_delay}s", file=sys.stderr)

        on_disconnect(last_block)
        await asyncio.sleep(min(reconnect_delay, 60))
        reconnect_delay = min(reconnect_delay * 2, 60)


class WsMonitor:
    """
    WebSocket 监控器
    在独立线程中运行 asyncio event loop，通过回调推送 log 到主线程队列
    """

    def __init__(self, config, contracts):
        self.config = config
        self.contracts = list(contracts)
        self.wss_url = _build_wss_url(config)
        self._log_queue = []
        self._lock = threading.Lock()
        self._thread = None
        self._stop_event = threading.Event()
        self._connected = False
        self._last_ws_block = 0
        self._disconnect_time = 0

    @property
    def available(self):
        return self.wss_url is not None

    @property
    def connected(self):
        return self._connected

    @property
    def last_ws_block(self):
        return self._last_ws_block

    def _on_log(self, log):
        # 连接信号（不放入队列）
        if log.get("_ws_connected"):
            self._connected = True
            self._disconnect_time = 0
            return
        block = int(log.get("blockNumber", "0x0"), 16)
        if block > self._last_ws_block:
            self._last_ws_block = block
        self._connected = True
        self._disconnect_time = 0
        with self._lock:
            self._log_queue.append(log)

    def _on_disconnect(self, last_block):
        self._connected = False
        if self._disconnect_time == 0:
            self._disconnect_time = time.time()
        if last_block > self._last_ws_block:
            self._last_ws_block = last_block

    def drain_logs(self):
        """取出所有待处理的 logs（线程安全）"""
        with self._lock:
            logs = self._log_queue
            self._log_queue = []
        return logs

    def start(self):
        """在后台线程启动 WSS 订阅"""
        if not self.available:
            print("[ws] 无可用 WSS 端点", file=sys.stderr)
            return False

        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(
                _ws_subscribe(self.wss_url, self.contracts,
                             self._on_log, self._on_disconnect, self._stop_event)
            )

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        print(f"[ws] 后台 WSS 线程已启动", file=sys.stderr)
        return True

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def should_fallback_to_poll(self, timeout=30):
        """是否应该降级到 HTTP 轮询（断线超过 timeout 秒）"""
        if self._connected:
            return False
        if self._disconnect_time == 0:
            return True  # 从未连接成功
        return (time.time() - self._disconnect_time) > timeout
