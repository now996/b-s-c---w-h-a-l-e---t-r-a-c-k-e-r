# BSC Whale Tracker 优化方案

基于全量代码审查，按优先级和模块分类，共发现 90+ 个问题。
以下按「影响大 + 修复成本合理」排序，分 4 批实施。

---

## 第一批：关键 Bug 修复（必须修，不修会出大问题）

### 1. 🔴 WSS 模式下多合约只第一个能收到事件
- **文件**: ws_monitor.py:171-176, monitor.py:710
- **现象**: `drain_logs()` 取出所有 logs 后清空队列，monitor.py 对每个合约分别调用，第一个合约取走全部，后续合约 ws_logs 为空
- **影响**: 同时监控多个合约时，只有第一个有实时数据
- **修复**: 主循环开始时调一次 `drain_logs()` 存入变量，每个合约按地址过滤

### 2. 🔴 Gas 告警条件过严，遗漏大额充值
- **文件**: monitor.py:607
- **现象**: 只有 `bnb_balance <= 0 and new_bnb > 0` 才触发，已有小额余额后不告警
- **影响**: 庄家地址已有 0.001 BNB 后又收到 10 BNB Gas → 不告警
- **修复**: 改为增量检测 `bnb_increase = new_bnb - old_bnb`，增量超阈值即触发

### 3. 🔴 Nonce 告警同样过严
- **文件**: monitor.py:623
- **现象**: 只在 nonce 从 0 变为 >0 时触发，已有交易历史的地址激活不告警
- **修复**: 检查 nonce 任何增长

### 4. 🔴 float() 解析大额 token 金额精度丢失
- **文件**: scan_core.py:187,217
- **现象**: `amount = float(value)` 对科学计数法大数（如 1e23）丢失精度
- **影响**: 庄家持仓计算偏差，千万级持仓可能差几万
- **修复**: 统一用 `int(hex_val, 16) / (10 ** decimals)` 或 `Decimal`

### 5. 🔴 LP 抛压估算逻辑错误
- **文件**: scan_core.py:1034-1036
- **现象**: LP 合约的 balance 是负数（LP 卖出量），取 abs 得到的是"LP 已卖出"而非"LP 池中剩余"
- **影响**: 抛压评估严重不准确
- **修复**: 使用链上 balanceOf 查询 LP 池实际持仓

### 6. 🔴 mint 事件余额计算缺失
- **文件**: scan_core.py:829-835
- **现象**: identify_whales 排除了 from=0x0 的 mint 事件，导致从零地址铸出的代币不计入余额
- **影响**: 某些代币的庄家持仓被低估
- **修复**: mint 事件的 to_addr 应正常累加

### 7. 🔴 API 密钥硬编码在源码中
- **文件**: scan_core.py:18, fund_trace.py:16, lp_detect.py:16, new_token_scanner.py:16
- **现象**: NodeReal API key `64a9df0874fb4a93b9d0a3849de012d3` 硬编码在 DEFAULT_RPCS
- **影响**: 已推送到 GitHub，密钥暴露
- **修复**: 移除含 key 的 URL，改为从 config.json 读取

---

## 第二批：性能优化（修完性能提升明显）

### 8. 🟡 RPC 层统一 + 粘性 RPC
- **现状**: `DEFAULT_RPCS` 和 `rpc_call()` 在 6 个文件中重复定义
- **修复**: 全部统一到 `data_source.py`，实现"粘性 RPC"（记住上次成功的节点）
- **收益**: 消除重复代码 ~200 行，RPC 调用延迟降低 30-50%

### 9. 🟡 庄家成本分析 O(W×N) → O(N)
- **文件**: scan_core.py:983-1007
- **现状**: 20 个庄家 × 数十万条记录双重循环
- **修复**: 预构建 addr→records 倒排索引
- **收益**: 大合约分析速度提升 10-20x

### 10. 🟡 get_token_info 5 次串行 RPC → 并行
- **文件**: scan_core.py:125-131
- **现状**: name/symbol/decimals/supply/owner 5 次串行调用，1-2.5s
- **修复**: ThreadPoolExecutor 并行
- **收益**: 缩短到 200-500ms

### 11. 🟡 Top50 合约检查串行 → 并行
- **文件**: scan_core.py:1052-1056
- **现状**: 50 个地址串行 eth_getCode，10-25s
- **修复**: 线程池并行，每批 10 个
- **收益**: 缩短到 1-3s

### 12. 🟡 monitored 地址余额查询串行 → 并行
- **文件**: monitor.py:596-641
- **现状**: 每个 watched 地址串行 2 次 RPC
- **修复**: ThreadPoolExecutor 并行查询
- **收益**: 监控 10 个地址从 ~5s 降到 ~0.5s

### 13. 🟡 无 Swap 时跳过 LP reserves 查询
- **文件**: monitor.py:813-836
- **现状**: 每轮每个 pair 都查 LP 储备，即使无交易
- **修复**: 仅该区间有 Swap 事件时才检查 LP
- **收益**: RPC 调用量减少 50-80%

### 14. 🟡 价格线性插值精度不足
- **文件**: scan_core.py:958-967
- **现状**: 仅 3 个采样点线性回归，误差可达数小时
- **修复**: 增加到 10 个采样点，分段插值
- **收益**: 历史成本计算精度大幅提升

---

## 第三批：功能完善（增强分析能力）

### 15. 🟢 LP 风险纳入 risk_score
- **现状**: lp_detect.py 检测了 LP 操纵（单提供者、快速撤池等），但 risk_score 完全没使用
- **修复**: 在 calculate_risk_score 中添加 LP 风险维度（0-10 分）

### 16. 🟢 删除废弃模块 data_cache.py
- **现状**: 与 db.py 完全重复，从未被调用
- **修复**: 直接删除

### 17. 🟢 集成孤岛模块
- **rhythm.py**: 从未在主流程调用，依赖的 JSON 文件不存在
- **snapshot.py**: 快照功能完整但无人调用
- **new_token_scanner.py**: 扫描结果不触发自动分析
- **修复**: 在 run_analysis / main.py 中接入

### 18. 🟢 quick_scan 输出补全
- **现状**: print_full_report 只输出基本分析，缺少分仓/聚类/标签/风险评分/跨合约/资金溯源/聪明钱/LP 分析
- **修复**: 补充所有分析模块的输出

### 19. 🟢 Webhook 支持 Markdown 格式
- **现状**: 钉钉/飞书/企业微信全用纯文本，格式丢失
- **修复**: 各平台使用各自的 Markdown 消息格式

### 20. 🟢 Telegram 403 永久禁用增加重置机制
- **现状**: 一旦 403 永远不重试，用户重新启用 Bot 也无法恢复
- **修复**: 每小时重试一次，成功则重置标志

### 21. 🟢 WSS 断线后补查缺失区块
- **文件**: ws_monitor.py:112-117
- **现状**: 断线 30s 内的 WSS logs 全部丢失
- **修复**: 重连后从 last_ws_block 补查

---

## 第四批：代码质量（降低维护成本）

### 22. ⚪ 魔法数字集中管理
- 20+ 个阈值散布在 labeler/shard_detect/cluster/smart_money/whale_alert/risk_score 中
- 移到 config.json 或独立 thresholds.py

### 23. ⚪ run_analysis 拆分
- 当前 300+ 行，职责不清
- 拆为 fetch_data / analyze_whales / analyze_pressure / analyze_distribution

### 24. ⚪ db.py 与 scan_core._init_db 合并
- 两份完全相同的数据库初始化逻辑

### 25. ⚪ is_contract_address 提取为公共工具
- 避免 cluster.py 函数内 from scan_core import 的循环依赖

### 26. ⚪ 散户阈值改为动态
- 硬编码 >1000 token，应改为基于 USD 价值

### 27. ⚪ 合约地址格式校验
- 入口函数添加 0x + 40 hex 校验

### 28. ⚪ 数据库文件名碰撞风险
- 前 10 位 (32 bit) 理论可碰撞，改为前 20 位或完整地址

---

## 预期收益汇总

| 批次 | 修复数 | 预期效果 |
|------|--------|---------|
| 第一批 | 7 项 | 修复功能 bug + 安全问题，WSS/监控/分析结果准确性大幅提升 |
| 第二批 | 7 项 | 分析速度提升 5-10x，监控 RPC 用量减少 50%+ |
| 第三批 | 7 项 | 功能完整度从 ~60% 提升到 ~95% |
| 第四批 | 7 项 | 代码量减少 ~300 行，可维护性显著提升 |
