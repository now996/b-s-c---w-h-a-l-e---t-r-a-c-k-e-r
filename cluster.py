#!/usr/bin/env python3
"""
cluster.py — 庄家关联聚类
通过共用子地址、资金流向、时间相关性，把多个庄家地址归为同一实体
"""
from collections import defaultdict


class UnionFind:
    """并查集"""
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[px] = py


def cluster_whales(records, whale_addrs, shard_results, pools_info):
    """
    聚类庄家地址。
    关联规则：
    1. 共用子地址（分仓到同一个地址）
    2. 直接互转（庄家之间有大额转账）
    3. 共用资金来源（从同一个地址收到初始资金）
    返回: [{
        "id": int,
        "members": [addr, ...],
        "links": [(addr1, addr2, reason), ...],
        "label": str,
    }]
    """
    if isinstance(pools_info, str):
        pool_set = {pools_info.lower()}
    else:
        pool_set = {p[0].lower() for p in pools_info}

    ZERO = "0x0000000000000000000000000000000000000000"
    DEAD = "0x000000000000000000000000000000000000dead"
    exclude = {ZERO, DEAD} | pool_set
    whale_set = {a.lower() for a in whale_addrs}

    uf = UnionFind()
    links = []  # (addr1, addr2, reason)

    # 规则1: 共用子地址
    shard_to_whales = defaultdict(set)  # shard_addr -> {whale_addrs}
    for whale, data in shard_results.items():
        for s in data.get("shards", []):
            shard_to_whales[s["addr"]].add(whale)

    for shard_addr, whales in shard_to_whales.items():
        whales = list(whales)
        for i in range(len(whales)):
            for j in range(i + 1, len(whales)):
                uf.union(whales[i], whales[j])
                short_shard = f"{shard_addr[:6]}..{shard_addr[-4:]}"
                links.append((whales[i], whales[j], f"共用子地址 {short_shard}"))

    # 规则2: 庄家之间直接互转
    whale_transfers = defaultdict(lambda: defaultdict(float))
    for block, fa, ta, amount in records:
        if fa in whale_set and ta in whale_set and fa != ta:
            whale_transfers[fa][ta] += amount

    for src, dsts in whale_transfers.items():
        for dst, amount in dsts.items():
            if amount > 0:
                uf.union(src, dst)
                links.append((src, dst, f"直接转账 {amount:,.0f}枚"))

    # 规则3: 共用 EOA 资金来源（排除合约地址，它们已在分仓检测中体现）
    from scan_core import is_contract_address
    funding_sources = defaultdict(lambda: defaultdict(float))
    for block, fa, ta, amount in records:
        if ta in whale_set and fa not in exclude and fa not in whale_set:
            funding_sources[fa][ta] += amount

    for source, funded in funding_sources.items():
        # 只看 EOA 来源，排除合约（分仓合约会把所有人串起来）
        big_funded = {w: amt for w, amt in funded.items() if amt > 10000}
        if len(big_funded) >= 2:
            if is_contract_address(source):
                continue  # 跳过合约来源
            funded_list = list(big_funded.keys())
            for i in range(len(funded_list)):
                for j in range(i + 1, len(funded_list)):
                    uf.union(funded_list[i], funded_list[j])
                    short_src = f"{source[:6]}..{source[-4:]}"
                    links.append((funded_list[i], funded_list[j], f"共用资金源 {short_src}"))

    # 构建聚类结果
    clusters_map = defaultdict(list)
    for addr in whale_set:
        root = uf.find(addr)
        clusters_map[root].append(addr)

    clusters = []
    for i, (root, members) in enumerate(sorted(clusters_map.items(), key=lambda x: -len(x[1]))):
        # 只输出有关联的组（>1个成员）
        cluster_links = [(a, b, r) for a, b, r in links if a in set(members) or b in set(members)]
        # 去重
        seen = set()
        unique_links = []
        for a, b, r in cluster_links:
            key = tuple(sorted([a, b])) + (r,)
            if key not in seen:
                seen.add(key)
                unique_links.append((a, b, r))

        if len(members) > 1:
            label = f"庄家团伙#{i+1} ({len(members)}个地址)"
        else:
            label = f"独立庄家"

        clusters.append({
            "id": i + 1,
            "members": sorted(members),
            "links": unique_links,
            "label": label,
            "size": len(members),
        })

    clusters.sort(key=lambda x: -x["size"])
    return clusters


def format_cluster_report(clusters):
    """格式化聚类报告（微信友好）"""
    lines = []
    lines.append("🕸️ 庄家关联")

    multi = [c for c in clusters if c["size"] > 1]
    solo = [c for c in clusters if c["size"] == 1]

    if not multi:
        lines.append("  未发现庄家关联，均为独立地址")
        return "\n".join(lines)

    for c in multi:
        lines.append(f"\n  {c['label']}")
        for addr in c["members"]:
            short = f"{addr[:6]}..{addr[-4:]}"
            lines.append(f"    {short}")
        # 显示关联原因（去重后最多5条）
        reasons_shown = set()
        for a, b, reason in c["links"][:5]:
            if reason not in reasons_shown:
                lines.append(f"    ↔ {reason}")
                reasons_shown.add(reason)

    if solo:
        lines.append(f"\n  独立庄家: {len(solo)}个")

    return "\n".join(lines)
