# BSC Whale Tracker

BSC 链 Meme 代币庄家分析与实时监控工具。

## 核心能力

- **一键扒庄** — 全量 ERC20 转账 + LP 行为 + 地址关联，还原庄家全貌
- **实时监控** — WebSocket 推送庄家买卖、LP 变化、分仓激活告警
- **6 种预警信号** — 吸筹/出货/加速/协同/转出/LP 异常
- **分仓停放追踪** — 监控"睡觉地址"的 gas 补充、nonce 变化、转仓出货
- **持仓快照** — Alchemy 批量查询庄家实时持仓，无需遍历全量转账
- **聪明钱识别** — 发现早期买入且盈利的地址，反向跟踪
- **多通道告警** — Telegram + 钉钉/飞书/企业微信 Webhook

## 架构

```
┌──────────────────────────────────────────────────┐
│  main.py (CLI)  │  monitor.py (实时监控)          │
├──────────────────────────────────────────────────┤
│  scan_core.py   │  whale_alert.py │  snapshot.py │
│  smart_money.py │  fund_trace.py  │  cluster.py  │
├──────────────────────────────────────────────────┤
│           data_source.py (统一数据源)             │
│  AlchemyClient │ PriceProvider │ RPCClient       │
├──────────────────────────────────────────────────┤
│  db.py (SQLite) │  notify.py (多通道通知)        │
└──────────────────────────────────────────────────┘
```

## 数据源

| 用途 | 主数据源 | 降级 |
|------|---------|------|
| 历史转账 | Alchemy `getAssetTransfers` | 公共 RPC `eth_getLogs` |
| 实时事件 | Alchemy WebSocket | HTTP 轮询 |
| 代币价格 | DexScreener | GeckoTerminal |
| BNB 价格 | Binance API | DexScreener |
| 持仓查询 | Alchemy `getTokenBalances` | — |
| K线历史 | GeckoTerminal OHLCV | — |

**智能模式选择：** 增量同步时，gap > 50000 blocks 自动走 Alchemy（快 10-50x），小增量走 `eth_getLogs`（更精确）。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

```bash
cp config.example.json config.json
# 编辑 config.json，填入：
# - alchemy_key: Alchemy BSC API Key
# - telegram_bot_token / telegram_chat_id: Telegram 通知（可选）
# - contracts: 要监控的代币合约
```

### 3. 运行

```bash
# 一键分析（单个代币）
python3 main.py --add-contract <合约地址> --name <代币名>
python3 main.py --analyze

# 启动实时监控
python3 main.py --monitor

# 只生成日报
python3 main.py --report

# 同步数据
python3 main.py --sync
```

### 4. 后台运行（推荐）

```bash
screen -dmS whale-tracker bash -c "ALCHEMY_KEY=your_key python3 main.py --monitor > logs/monitor.log 2>&1"
```

## 主要模块

| 文件 | 功能 |
|------|------|
| `main.py` | CLI 主入口，分析/监控/日报/同步 |
| `scan_core.py` | 扒庄核心分析引擎 |
| `monitor.py` | 实时监控（WSS + HTTP 双模式） |
| `ws_monitor.py` | WebSocket 事件推送 |
| `data_source.py` | 统一数据源抽象（Alchemy/DexScreener/RPC） |
| `notify.py` | 多通道通知（Telegram + Webhook） |
| `db.py` | 数据库管理 + 自动清理 |
| `snapshot.py` | 持仓快照（Alchemy getTokenBalances） |
| `whale_alert.py` | 6 种庄家预警信号 |
| `smart_money.py` | 聪明钱识别与追踪 |
| `fund_trace.py` | 资金溯源（CEX 提币等） |
| `cluster.py` | 地址聚类（关联团伙） |
| `labeler.py` | 地址标签 |
| `risk_score.py` | 风险评分 |
| `price_cache.py` | 价格缓存（SQLite） |
| `lp_detect.py` | LP 变化检测与操纵分析 |
| `shard_detect.py` | 分仓停放地址激活监控 |
| `new_token_scanner.py` | 新代币扫描 |
| `cross_track.py` | 跨合约追踪 |
| `rhythm.py` | 交易节奏分析 |
| `format_wechat.py` | 微信格式化报告 |
| `wechat_bridge.py` | 微信推送桥接 |

## 配置说明

关键配置项（`config.json`）：

```json
{
  "alchemy_key": "your_alchemy_bsc_key",
  "telegram_bot_token": "",
  "telegram_chat_id": "",
  "contracts": {
    "0x...": {
      "name": "代币名",
      "pair": "LP pair 地址",
      "whale_addrs": ["庄家地址列表"],
      "watched_wallets": ["分仓停放地址"],
      "alert_threshold_usd": 500
    }
  },
  "monitor_mode": "ws",
  "data_source": {
    "history": { "primary": "alchemy_transfers" },
    "price": { "primary": "dexscreener" }
  },
  "notify": {
    "webhook_url": "",
    "webhook_type": "dingtalk"
  },
  "db": {
    "cleanup_days": 30
  }
}
```

回退到旧方案：修改 `data_source.history.primary` 为 `"eth_getlogs"` 即可。

## 通知通道

| 通道 | 配置 | 说明 |
|------|------|------|
| **Telegram** | `telegram_bot_token` + `telegram_chat_id` | 支持 HTML 格式，403 自动永久禁用 |
| **钉钉** | `notify.webhook_url` + `webhook_type: "dingtalk"` | Webhook |
| **飞书** | `webhook_type: "feishu"` | Webhook |
| **企业微信** | `webhook_type: "wecom"` | Webhook |

## 预警信号

| 信号 | 触发条件 |
|------|---------|
| 🟢 庄家吸筹 | 1h 净买入 > $5000，买/卖比 > 3x |
| 🔴 庄家出货 | 1h 净卖出 > $5000，卖/买比 > 3x |
| ⚠️ 庄家大量转出 | 1h 转出代币价值 > 1M * price |
| 🟡 买入加速 | 近30min 买入 > 前30min * 3x |
| 🔴 卖出加速 | 近30min 卖出 > 前30min * 3x |
| 🟢🟢 多庄协同买入 | 3+ 庄家同时买入 |
| 🔴🔴 多庄协同卖出 | 3+ 庄家同时卖出 |

## License

MIT
