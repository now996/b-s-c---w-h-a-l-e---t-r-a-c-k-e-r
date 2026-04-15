"""
data_cache.py — SQLite 数据缓存，避免重复拉取链上数据
"""
import sqlite3
import os
import sys
import time
import requests
import json

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "whale_tracker.db")


def get_db():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS transfers (
        tx_hash TEXT, block INTEGER, timestamp TEXT,
        from_addr TEXT, to_addr TEXT, amount REAL, contract TEXT,
        PRIMARY KEY (tx_hash, from_addr, to_addr)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sync_state (
        contract TEXT PRIMARY KEY, last_page_key TEXT, total_records INTEGER,
        last_sync TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS internal_transfers (
        tx_hash TEXT, block INTEGER, timestamp TEXT,
        from_addr TEXT, to_addr TEXT, value_bnb REAL,
        addr TEXT,
        PRIMARY KEY (tx_hash, from_addr, to_addr)
    )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_internal_addr ON internal_transfers(addr)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_contract ON transfers(contract)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_from ON transfers(from_addr)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_to ON transfers(to_addr)""")
    conn.commit()
    return conn


def get_sync_state(conn, contract):
    row = conn.execute("SELECT last_page_key, total_records FROM sync_state WHERE contract=?",
                       (contract.lower(),)).fetchone()
    return (row[0], row[1]) if row else (None, 0)


def update_sync_state(conn, contract, page_key, total):
    conn.execute("""INSERT OR REPLACE INTO sync_state (contract, last_page_key, total_records, last_sync)
                    VALUES (?, ?, ?, datetime('now'))""",
                 (contract.lower(), page_key, total))
    conn.commit()


def insert_transfers(conn, records, contract):
    if not records:
        return 0
    changes_before = conn.total_changes
    for r in records:
        conn.execute("""INSERT OR IGNORE INTO transfers
            (tx_hash, block, timestamp, from_addr, to_addr, amount, contract)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (r["hash"], r.get("block", 0), r["ts"],
             r["from"], r["to"], r["amount"], contract.lower()))
    conn.commit()
    return conn.total_changes - changes_before


def sync_transfers(alchemy_key, contract, full=False):
    """增量同步转账记录到 SQLite"""
    conn = get_db()
    try:
        contract = contract.lower()
        alchemy_url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"

        page_key, total = get_sync_state(conn, contract)
        if full:
            page_key = None
            total = 0

        print(f"[cache] 同步 {contract[:10]}... 从 page_key={str(page_key)[:20] if page_key else 'START'}", file=sys.stderr)

        new_records = 0
        page = 0
        consecutive_failures = 0

        while True:
            page += 1
            params = {
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [{
                    "fromBlock": "0x0", "toBlock": "latest",
                    "contractAddresses": [contract],
                    "category": ["erc20"],
                    "maxCount": "0x3e8",
                    "order": "asc",
                    "withMetadata": True,
                }],
                "id": 1
            }
            if page_key:
                params["params"][0]["pageKey"] = page_key

            data = {}
            for retry in range(3):
                try:
                    r = requests.post(alchemy_url, json=params, timeout=30)
                    data = r.json()
                    break
                except Exception:
                    time.sleep(2)

            if not data or (not data.get("result") and "error" not in data):
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    print(f"[cache] 连续 {consecutive_failures} 次API无响应，中止同步", file=sys.stderr)
                    break
                continue

            if "error" in data:
                print(f"[cache] API error page {page}: {data['error']}", file=sys.stderr)
                break

            consecutive_failures = 0
            result = data.get("result") or {}
            transfers = result.get("transfers") or []

            records = []
            for tx in transfers:
                from_addr = (tx.get("from") or "").lower()
                to_addr = (tx.get("to") or "").lower()
                if not from_addr or not to_addr:
                    continue
                value = tx.get("value")
                if value is not None:
                    try:
                        amount = float(value)
                    except Exception:
                        continue
                else:
                    raw = tx.get("rawContract") or {}
                    hex_val = raw.get("value") or "0x0"
                    decimals = raw.get("decimals")
                    try:
                        raw_int = int(hex_val, 16)
                        if decimals is not None:
                            amount = raw_int / (10 ** int(decimals))
                        else:
                            amount = raw_int / 1e18
                    except Exception:
                        continue
                if amount <= 0:
                    continue
                meta = tx.get("metadata") or {}
                records.append({
                    "hash": tx.get("hash") or "",
                    "block": int(tx.get("blockNum") or "0x0", 16),
                    "ts": meta.get("blockTimestamp") or "",
                    "from": from_addr,
                    "to": to_addr,
                    "amount": amount,
                })

            inserted = insert_transfers(conn, records, contract)
            new_records += inserted
            total += len(transfers)
            page_key = result.get("pageKey")

            if page % 50 == 0:
                print(f"[cache] Page {page}: +{len(transfers)} (累计 {total}, 新增 {new_records})", file=sys.stderr)
                update_sync_state(conn, contract, page_key, total)

            if not page_key or not transfers:
                break

            if page % 5 == 0:
                time.sleep(0.3)

        update_sync_state(conn, contract, page_key, total)
        print(f"[cache] 同步完成: {total} 总记录, {new_records} 新增", file=sys.stderr)
    finally:
        conn.close()
    return total, new_records


def query_transfers(contract, from_addr=None, to_addr=None, limit=None):
    """查询缓存的转账记录"""
    conn = get_db()
    try:
        sql = "SELECT tx_hash, block, timestamp, from_addr, to_addr, amount FROM transfers WHERE contract=?"
        params = [contract.lower()]
        if from_addr:
            sql += " AND from_addr=?"
            params.append(from_addr.lower())
        if to_addr:
            sql += " AND to_addr=?"
            params.append(to_addr.lower())
        sql += " ORDER BY block ASC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [{"hash": r[0], "block": r[1], "ts": r[2], "from": r[3], "to": r[4], "amount": r[5]} for r in rows]


def query_all_transfers(contract):
    """查询某合约全部转账"""
    return query_transfers(contract)


def get_total_records(contract):
    conn = get_db()
    try:
        row = conn.execute("SELECT COUNT(*) FROM transfers WHERE contract=?", (contract.lower(),)).fetchone()
    finally:
        conn.close()
    return row[0] if row else 0


def sync_internal_transfers(alchemy_key, addr, direction="to", max_pages=3):
    """同步某地址的 BNB 内部交易到 SQLite"""
    conn = get_db()
    try:
        addr = addr.lower()
        alchemy_url = f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
        page_key = None
        new_records = 0

        for page in range(1, max_pages + 1):
            params = {
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [{
                    f"{direction}Address": addr,
                    "category": ["internal"],
                    "maxCount": "0x3e8",
                    "order": "desc",
                    "withMetadata": True,
                }],
                "id": 1
            }
            if page_key:
                params["params"][0]["pageKey"] = page_key

            data = {}
            for retry in range(3):
                try:
                    r = requests.post(alchemy_url, json=params, timeout=30)
                    data = r.json()
                    break
                except Exception:
                    import time
                    time.sleep(2)

            if not data or "error" in data:
                break

            result = data.get("result") or {}
            transfers = result.get("transfers") or []

            for tx in transfers:
                from_addr = (tx.get("from") or "").lower()
                to_addr = (tx.get("to") or "").lower()
                if not from_addr or not to_addr:
                    continue
                value = float(tx.get("value") or 0)
                if value <= 0:
                    continue
                meta = tx.get("metadata") or {}
                try:
                    conn.execute("""INSERT OR IGNORE INTO internal_transfers
                        (tx_hash, block, timestamp, from_addr, to_addr, value_bnb, addr)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (tx.get("hash") or "",
                         int(tx.get("blockNum") or "0x0", 16),
                         meta.get("blockTimestamp") or "",
                         from_addr, to_addr, value, addr))
                    new_records += 1
                except Exception:
                    pass

            conn.commit()
            page_key = result.get("pageKey")
            if not page_key or not transfers:
                break

        return new_records
    finally:
        conn.close()


def query_internal_transfers(addr, direction=None):
    """查询某地址的内部交易"""
    conn = get_db()
    try:
        if direction == "to":
            sql = "SELECT tx_hash, block, timestamp, from_addr, to_addr, value_bnb FROM internal_transfers WHERE to_addr=? ORDER BY block DESC"
            rows = conn.execute(sql, (addr.lower(),)).fetchall()
        elif direction == "from":
            sql = "SELECT tx_hash, block, timestamp, from_addr, to_addr, value_bnb FROM internal_transfers WHERE from_addr=? ORDER BY block DESC"
            rows = conn.execute(sql, (addr.lower(),)).fetchall()
        else:
            sql = "SELECT tx_hash, block, timestamp, from_addr, to_addr, value_bnb FROM internal_transfers WHERE addr=? ORDER BY block DESC"
            rows = conn.execute(sql, (addr.lower(),)).fetchall()
    finally:
        conn.close()
    return [{"hash": r[0], "block": r[1], "ts": r[2], "from": r[3], "to": r[4], "value_bnb": r[5]} for r in rows]
