# Changelog

## Unreleased

### Added
- 新增 `watched_wallets` 停放地址监控配置
- 新增 `watched_labels` 地址标签配置
- 新增停放地址首次收到 BNB gas 告警
- 新增停放地址 nonce 从 0 激活到 >0 的告警
- 新增停放地址转出目标代币告警
- 新增停放地址直接卖进 LP 的高优先级告警
- 新增 `logs/watched_state.json` 用于持久化 watched 地址监控状态，避免重启后重复报警
- 新增 `logs/monitor_state.json` 用于持久化监控进度与状态
- 新增 watched 地址阶段状态：`静止 / 观察中 / 已激活 / 已转仓 / 已出货`
- 新增 `main.py --add-contract <合约> [--name 名称]` 一键接入监控配置
- 新增运行中监控对 `config.json` 的热重载，配置写入后无需手动重启监控

### Improved
- `monitor.py` 增加官方 BNB Chain RPC 优先 + 多节点 fallback
- `monitor.py` 增加 RPC 失明告警与恢复提示
- `monitor.py` 增加断点续跑，重启后不再固定从 `latest-10` 硬起
- `monitor.py` 增加 catch-up 模式，掉线后按更大 chunk 快速追平 backlog
- `monitor.py` 改为按天切分事件日志并支持自动清理
- 价格源不可用时仍继续记录链上事件，避免整轮白跑
- `scan_core.py` 统一到多 RPC fallback 体系
- `lp_detect.py` 统一到多 RPC fallback 体系
- `fund_trace.py` 统一到多 RPC fallback 体系
- `new_token_scanner.py` 统一到多 RPC fallback 体系
- `legacy/whale_tracker.py` 统一到多 RPC fallback 体系
- README 增加“分仓停放地址激活监控”与配置说明
- 配置模板增加 watched wallet / core address 示例字段

### Notes
- 本次更新主要覆盖监控稳定性、热重载能力、watched 地址生命周期跟踪，以及分析链路的 RPC 统一
- 未提交真实 `config.json`、日志、缓存数据库、API key、Telegram token 等敏感内容
