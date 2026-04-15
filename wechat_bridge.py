#!/usr/bin/env python3
"""
wechat_bridge.py — 微信告警桥接
监控进程写入 pending_alerts.jsonl，heartbeat 调用 flush_alerts() 读取并清空
"""
import os
import json
import time
import fcntl

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ALERTS_FILE = os.path.join(SCRIPT_DIR, "logs", "pending_alerts.jsonl")


def push_alert(message):
    """监控进程调用：写入一条待推送告警"""
    os.makedirs(os.path.dirname(ALERTS_FILE), exist_ok=True)
    with open(ALERTS_FILE, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(json.dumps({"msg": message, "ts": time.time()}, ensure_ascii=False) + "\n")
        fcntl.flock(f, fcntl.LOCK_UN)


def flush_alerts(max_count=10):
    """heartbeat 调用：原子读取并清空待推送告警，返回消息列表"""
    if not os.path.exists(ALERTS_FILE):
        return []

    alerts = []
    try:
        # 用 r+ 模式打开，在同一个锁内完成读取和截断，避免竞态丢失告警
        with open(ALERTS_FILE, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            for line in f:
                line = line.strip()
                if line:
                    try:
                        alerts.append(json.loads(line)["msg"])
                    except Exception:
                        pass
            # 在同一个锁内截断文件
            f.seek(0)
            f.truncate()
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception:
        pass

    return alerts[:max_count]


if __name__ == "__main__":
    # 测试
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "flush":
        alerts = flush_alerts()
        if alerts:
            for a in alerts:
                print(a)
                print("---")
        else:
            print("无待推送告警")
    else:
        push_alert("🧪 测试告警：微信桥接正常工作")
        print("已写入测试告警")
