#!/usr/bin/env python3
"""
db.py — 统一数据库管理
- 自动清理旧数据
- whale_snapshot 表（持仓快照）
- Schema 自动迁移
"""
import os
import sys
import sqlite3
import time
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")


def get_db_path(contract):
    """获取合约对应的数据库路径"""
    contract = contract.lower()
    return os.path.join(DATA_DIR, f"{contract[:20]}.db")


def get_connection(contract):
    """获取数据库连接（带 WAL 模式）"""
    db_path = get_db_path(contract)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(conn):
    """初始化数据库 schema（含自动迁移）"""
    # 主转账表
    conn.execute("""CREATE TABLE IF NOT EXISTS transfers (
        block INTEGER, from_addr TEXT, to_addr TEXT, amount REAL,
        tx_hash TEXT DEFAULT '')""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sync_state (
        id INTEGER PRIMARY KEY, page_key TEXT, total INTEGER,
        last_synced_block INTEGER DEFAULT 0, last_sync_ts TEXT DEFAULT '')""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_from ON transfers(from_addr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_to ON transfers(to_addr)")

    # 唯一索引（去重）
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup ON transfers(block, from_addr, to_addr, amount)")
    except sqlite3.IntegrityError:
        print("  [db] 清理重复数据...", file=sys.stderr)
        conn.execute("""DELETE FROM transfers WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM transfers GROUP BY block, from_addr, to_addr, amount)""")
        conn.commit()
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dedup ON transfers(block, from_addr, to_addr, amount)")

    # 庄家持仓快照表（新增）
    conn.execute("""CREATE TABLE IF NOT EXISTS whale_snapshot (
        contract TEXT, addr TEXT, balance REAL, usd_value REAL,
        token_price REAL, snapshot_ts TEXT,
        PRIMARY KEY (contract, addr, snapshot_ts))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_ts ON whale_snapshot(snapshot_ts)")

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
        print(f"  [db] 数据库迁移完成, last_synced_block={max_block}", file=sys.stderr)

    conn.commit()
    return conn


def cleanup_old_data(conn, contract, cleanup_days=30, keep_whale_addrs=None):
    """
    清理旧数据，释放磁盘空间
    - cleanup_days: 普通转账数据保留天数
    - keep_whale_addrs: 庄家地址列表，这些地址的转账永久保留
    """
    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=cleanup_days)
    cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M:%S")

    # 获取 cutoff 对应的 block（粗略估算）
    # BSC 平均 3 秒一个块
    try:
        row = conn.execute("SELECT last_synced_block FROM sync_state WHERE id=1").fetchone()
        last_block = row[0] if row else 0
    except Exception:
        last_block = 0

    if last_block <= 0:
        return 0

    # 估算 cutoff 区块号
    blocks_per_day = 86400 / 3  # ~28800
    cutoff_block = int(last_block - blocks_per_day * cleanup_days)

    if cutoff_block <= 0:
        return 0

    # 统计待清理数据量
    total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]

    if keep_whale_addrs:
        # 保留庄家地址相关的转账
        whale_placeholders = ",".join(["?"] * len(keep_whale_addrs))
        old_count = conn.execute(
            f"SELECT COUNT(*) FROM transfers WHERE block < ? AND from_addr NOT IN ({whale_placeholders}) AND to_addr NOT IN ({whale_placeholders})",
            [cutoff_block] + list(keep_whale_addrs) + list(keep_whale_addrs)
        ).fetchone()[0]
        if old_count > 0:
            conn.execute(
                f"DELETE FROM transfers WHERE block < ? AND from_addr NOT IN ({whale_placeholders}) AND to_addr NOT IN ({whale_placeholders})",
                [cutoff_block] + list(keep_whale_addrs) + list(keep_whale_addrs)
            )
    else:
        old_count = conn.execute("SELECT COUNT(*) FROM transfers WHERE block < ?", (cutoff_block,)).fetchone()[0]
        if old_count > 0:
            conn.execute("DELETE FROM transfers WHERE block < ?", (cutoff_block,))

    if old_count > 0:
        conn.commit()
        # VACUUM 释放磁盘空间
        try:
            conn.execute("VACUUM")
        except Exception:
            pass

    remaining = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    return old_count


def cleanup_snapshots(conn, keep_days=90):
    """清理过期的持仓快照"""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")
    deleted = conn.execute("DELETE FROM whale_snapshot WHERE snapshot_ts < ?", (cutoff,)).rowcount
    if deleted > 0:
        conn.commit()
    return deleted


def save_whale_snapshot(conn, contract, whale_balances, token_price):
    """保存庄家持仓快照

    whale_balances: {addr: balance, ...}
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    records = []
    for addr, balance in whale_balances.items():
        usd_value = balance * token_price
        records.append((contract.lower(), addr.lower(), balance, usd_value, token_price, now))

    conn.executemany(
        "INSERT OR REPLACE INTO whale_snapshot (contract, addr, balance, usd_value, token_price, snapshot_ts) VALUES (?,?,?,?,?,?)",
        records
    )
    conn.commit()
    return len(records)


def get_latest_snapshot(conn, contract):
    """获取最新的持仓快照"""
    contract = contract.lower()
    row = conn.execute(
        "SELECT MAX(snapshot_ts) FROM whale_snapshot WHERE contract = ?",
        (contract,)
    ).fetchone()
    if not row or not row[0]:
        return None, []

    latest_ts = row[0]
    rows = conn.execute(
        "SELECT addr, balance, usd_value, token_price, snapshot_ts FROM whale_snapshot WHERE contract = ? AND snapshot_ts = ?",
        (contract, latest_ts)
    ).fetchall()
    return latest_ts, rows


def compare_snapshots(conn, contract):
    """对比最近两次快照，返回持仓变化"""
    contract = contract.lower()
    timestamps = conn.execute(
        "SELECT DISTINCT snapshot_ts FROM whale_snapshot WHERE contract = ? ORDER BY snapshot_ts DESC LIMIT 2",
        (contract,)
    ).fetchall()

    if len(timestamps) < 2:
        return None

    new_ts, old_ts = timestamps[0][0], timestamps[1][0]

    new_rows = conn.execute(
        "SELECT addr, balance FROM whale_snapshot WHERE contract = ? AND snapshot_ts = ?",
        (contract, new_ts)
    ).fetchall()

    old_rows = conn.execute(
        "SELECT addr, balance FROM whale_snapshot WHERE contract = ? AND snapshot_ts = ?",
        (contract, old_ts)
    ).fetchall()

    old_map = {r[0]: r[1] for r in old_rows}
    changes = []
    for addr, new_bal in new_rows:
        old_bal = old_map.get(addr, 0)
        diff = new_bal - old_bal
        if abs(diff) > 0.001:
            changes.append({
                "addr": addr,
                "old_balance": old_bal,
                "new_balance": new_bal,
                "diff": diff,
            })

    return {
        "old_ts": old_ts,
        "new_ts": new_ts,
        "changes": changes,
    }


def get_db_stats(contract=None):
    """获取数据库统计信息"""
    stats = {}
    if contract:
        # 支持传入完整合约地址或前10位缩写
        contract_key = contract.lower()[:20]
        contracts = [contract_key]
    else:
        # 扫描所有 db 文件
        contracts = []
        if os.path.exists(DATA_DIR):
            for f in os.listdir(DATA_DIR):
                if f.endswith(".db") and not f.startswith("whale_tracker") and not f.startswith("price_cache"):
                    contracts.append(f.replace(".db", ""))

    for c in contracts:
        db_path = os.path.join(DATA_DIR, f"{c}.db")
        if not os.path.exists(db_path):
            continue
        try:
            conn = sqlite3.connect(db_path)
            rec_count = 0
            try:
                rec_count = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
            except Exception:
                pass
            max_block = 0
            try:
                max_block = conn.execute("SELECT MAX(block) FROM transfers").fetchone()[0] or 0
            except Exception:
                pass
            row = None
            try:
                row = conn.execute("SELECT last_synced_block, last_sync_ts FROM sync_state WHERE id=1").fetchone()
            except Exception:
                pass
            snap_count = 0
            try:
                snap_count = conn.execute("SELECT COUNT(*) FROM whale_snapshot").fetchone()[0]
            except Exception:
                pass
            conn.close()
            file_size = os.path.getsize(db_path)
            stats[c] = {
                "records": rec_count,
                "max_block": max_block,
                "last_synced_block": row[0] if row else 0,
                "last_sync_ts": row[1] if row else "",
                "snapshots": snap_count,
                "file_size_mb": round(file_size / 1024 / 1024, 2),
            }
        except Exception as e:
            stats[c] = {"error": str(e)}

    return stats
