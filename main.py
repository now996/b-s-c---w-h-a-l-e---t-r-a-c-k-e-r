#!/usr/bin/env python3
"""
main.py — BSC 庄家追踪系统主控
用法:
  python3 main.py                     # 完整系统（监控 + 日报）
  python3 main.py --analyze           # 只跑一次分析
  python3 main.py --monitor           # 只启动监控
  python3 main.py --report            # 只生成一次报告
  python3 main.py --sync              # 只同步数据
  python3 main.py --add-contract <合约> [--name 名称]   # 一键接入监控配置
"""
import sys
import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from scan_core import run_analysis, load_transfers, get_token_price, ALCHEMY_KEY
from format_wechat import format_wechat_report
from monitor import run_monitor
from notify import create_notifier


CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
        f.write("\n")


def notify_fn_factory(config):
    """创建通知函数 — 使用统一通知模块（Telegram + Webhook 多通道）"""
    return create_notifier(config)


def _ensure_alchemy_env(config):
    """确保 ALCHEMY_KEY 环境变量已设置，并 reload scan_core"""
    key = config.get("alchemy_key", "")
    if key:
        os.environ["ALCHEMY_KEY"] = key
    import importlib
    import scan_core
    importlib.reload(scan_core)


def do_sync(config):
    """同步所有合约的链上数据"""
    _ensure_alchemy_env(config)
    import scan_core
    for contract in config["contracts"]:
        name = config["contracts"][contract]["name"]
        print(f"[sync] {name} 同步中...", file=sys.stderr)
        records = scan_core.load_transfers(contract)
        print(f"[sync] {name} 完成: {len(records)} 条", file=sys.stderr)


def do_analyze(config):
    """运行完整分析并打印结果（使用 scan_core v2）"""
    _ensure_alchemy_env(config)
    import scan_core

    for contract, cfg in config["contracts"].items():
        name = cfg["name"]
        print(f"\n[analyze] {name} 分析中...", file=sys.stderr)
        result = scan_core.run_analysis(contract)

        if "error" in result:
            print(f"[analyze] {name} 错误: {result['error']}", file=sys.stderr)
            continue

        report = format_wechat_report(
            result["info"], result["token_price"], result["dex_data"],
            result["whale_results"], result["pressure"], result["concentration"],
            result["top_holders"], result["retail"], result["risks"],
            result["total_records"], result["total_holders"], len(result["whale_addrs"])
        )
        print(report)

        from quick_scan import print_full_report
        print_full_report(result)


def do_report(config):
    """生成报告并推送"""
    _ensure_alchemy_env(config)
    import scan_core

    notify = create_notifier(config)

    for contract, cfg in config["contracts"].items():
        name = cfg["name"]
        print(f"[report] {name} 生成报告...", file=sys.stderr)
        result = scan_core.run_analysis(contract)

        if "error" in result:
            print(f"[report] {name} 错误: {result['error']}", file=sys.stderr)
            continue

        tg_report = f"📊 <b>日报 [{name}]</b>\n\n"
        tg_report += f"价格: ${result['token_price']:.8f}\n"
        if result["dex_data"]:
            tg_report += f"24h涨跌: {result['dex_data'].get('priceChange', {}).get('h24', 0)}%\n"
            tg_report += f"24h量: ${float(result['dex_data'].get('volume', {}).get('h24', 0)):,.0f}\n"
            tg_report += f"流动性: ${float(result['dex_data'].get('liquidity', {}).get('usd', 0)):,.0f}\n"

        p = result["pressure"]
        tg_report += f"\n庄家剩余: {p['remaining']:,.0f} ({p['pct_supply']:.1f}%)\n"
        tg_report += f"全抛影响: -{p['impact_pct']:.1f}%\n"

        if result["risks"]:
            tg_report += "\n⚠️ " + " | ".join(result["risks"])

        notify(tg_report)
        print(f"[report] {name} 日报已推送", file=sys.stderr)


def add_contract(config, contract, display_name=None):
    """一键把新合约接入监控配置"""
    _ensure_alchemy_env(config)
    import scan_core

    contract = contract.lower()
    if contract in config.get("contracts", {}):
        return {"status": "exists", "contract": contract, "name": config["contracts"][contract].get("name")}

    result = scan_core.run_analysis(contract)
    if "error" in result:
        return {"status": "error", "error": result["error"]}

    info = result["info"]
    whales = result.get("whale_addrs", []) or []
    pair = info.get("pair")
    if not pair:
        return {"status": "error", "error": "未找到 LP pair"}

    name = display_name or info.get("name") or info.get("symbol") or contract[:10]
    whale_labels = result.get("whale_labels", {}) or {}
    watched_wallets = whales[: min(5, len(whales))]
    watched_labels = {}
    for i, addr in enumerate(watched_wallets, 1):
        tags = whale_labels.get(addr, [])
        watched_labels[addr] = (" ".join(tags[:2]) if tags else f"重点地址{i}")[:40]

    priority_core_addresses = {}
    for addr in whales[:2]:
        tags = whale_labels.get(addr, [])
        priority_core_addresses[addr] = (" ".join(tags[:2]) if tags else "核心地址")[:40]

    # 构建多 pair 列表
    pools = info.get("pools", [])
    pairs_list = []
    for pool_addr, pool_ver in pools:
        pairs_list.append({"pair": pool_addr, "dex": pool_ver})
    if not pairs_list and pair:
        pairs_list = [{"pair": pair, "dex": "PancakeSwap-v2"}]

    config.setdefault("contracts", {})[contract] = {
        "name": name,
        "pair": pair,
        "pairs": pairs_list,
        "whale_addrs": whales,
        "watched_wallets": watched_wallets,
        "watched_labels": watched_labels,
        "priority_core_addresses": priority_core_addresses,
        "alert_threshold_usd": 500,
    }
    save_config(config)
    return {
        "status": "ok",
        "contract": contract,
        "name": name,
        "pair": pair,
        "whales": len(whales),
        "watched_wallets": watched_wallets,
    }


def daily_report_scheduler(_initial_config):
    """每天 UTC 0:00 (= UTC+8 8:00) 自动生成日报，每次重新加载配置"""
    while True:
        now = datetime.now(timezone.utc)
        # 计算下一个 UTC 0:00
        target = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait = (target - now).total_seconds()
        print(f"[scheduler] 下次日报: {wait/3600:.1f}h 后", file=sys.stderr)
        time.sleep(wait)

        try:
            # 每次重新加载配置，确保热重载生效
            config = load_config()
            do_report(config)
        except Exception as e:
            print(f"[scheduler] 日报错误: {e}", file=sys.stderr)


def main():
    config = load_config()
    args = sys.argv[1:]

    if config.get("alchemy_key"):
        os.environ["ALCHEMY_KEY"] = config["alchemy_key"]

    if "--add-contract" in args:
        idx = args.index("--add-contract")
        if idx + 1 >= len(args):
            print("用法: python3 main.py --add-contract <合约地址> [--name 名称]")
            return
        contract = args[idx + 1]
        display_name = None
        if "--name" in args:
            nidx = args.index("--name")
            if nidx + 1 < len(args):
                display_name = args[nidx + 1]
        result = add_contract(config, contract, display_name)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if "--sync" in args:
        do_sync(config)
        return

    if "--analyze" in args:
        do_analyze(config)
        return

    if "--report" in args:
        do_report(config)
        return

    if "--monitor" in args:
        notify = notify_fn_factory(config)
        notify("🚀 庄家监控已启动")
        run_monitor(config, notify)
        return

    print("[main] BSC 庄家追踪系统启动", file=sys.stderr)
    notify = notify_fn_factory(config)
    print("[main] 初始数据同步...", file=sys.stderr)
    do_sync(config)
    notify("🚀 BSC 庄家追踪系统已启动\n监控中...")
    report_thread = threading.Thread(target=daily_report_scheduler, args=(config,), daemon=True)
    report_thread.start()
    print("[main] 启动实时监控...", file=sys.stderr)
    run_monitor(config, notify)


if __name__ == "__main__":
    main()
