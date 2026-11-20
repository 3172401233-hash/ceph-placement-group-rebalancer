"""Ceph 风格归置组（PG）重平衡规划器（纯模拟器）。

本模块根据声明式的集群描述（OSD 权重、机架/主机故障域、现有 PG 映射、
容量、near-full 阈值、恢复带宽、并发迁移上限）计算一份**分阶段**的 PG
迁移计划。它只是规划/模拟：不会调用 ``ceph``、``ceph osd`` 等任何
Ceph 管理命令，也不连接真实集群。

设计要点：

- 目标分布遵守故障域隔离（主机、可选机架）与容量（near-full）约束；
- 单轮迁移字节数不超过恢复带宽预算，并限制单轮并发迁移数、
  同一源 OSD 与同一机架的并发迁移数；
- 降级 PG 与接近满盘的 OSD 优先处理；
- 同一个 PG 在整个计划中最多迁移一次，避免重复/来回移动；
- 输入无可行方案时抛出 :class:`InfeasibleError`，指明具体约束，
  绝不输出部分不安全的计划。
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class InfeasibleError(Exception):
    """输入不存在可行（安全）的重平衡方案。

    ``constraint`` 指明被违反的约束类别（如 ``failure-domain``、
    ``capacity``、``recovery-budget``、``input``），``detail`` 给出
    具体的 PG / OSD / 数值，便于定位。
    """

    def __init__(self, constraint: str, detail: str):
        self.constraint = constraint
        self.detail = detail
        super().__init__("[%s] %s" % (constraint, detail))


@dataclass
class OSD:
    osd_id: int
    host: str
    rack: str
    weight: float
    capacity: int          # 字节
    up: bool = True        # False 表示节点/OSD 故障


@dataclass
class PG:
    pg_id: str
    size: int              # 字节（单副本）
    osds: List[int]        # 现有副本映射
    degraded: bool = False


@dataclass
class PlannerConfig:
    near_full_ratio: float = 0.85   # OSD 使用率 near-full 阈值
    recovery_budget: int = 1 << 30  # 单轮恢复带宽预算（字节）
    max_parallel: int = 8           # 单轮最大并发迁移数
    max_per_source: int = 2         # 单轮同一源 OSD 最大迁移数
    max_per_rack: int = 4           # 单轮同一机架（源或目标）最大迁移数
    tolerance: float = 0.10         # 相对理想负载的容忍偏差
    enforce_rack: bool = True       # 是否强制机架级故障域隔离


@dataclass
class Move:
    pg_id: str
    src: int
    dst: int
    size: int


@dataclass
class Round:
    index: int
    moves: List[Move] = field(default_factory=list)

    @property
    def bytes_moved(self) -> int:
        return sum(m.size for m in self.moves)


@dataclass
class Plan:
    rounds: List[Round]
    final_mapping: Dict[str, List[int]]

    @property
    def moves(self) -> List[Move]:
        return [m for r in self.rounds for m in r.moves]

    @property
    def total_bytes(self) -> int:
        return sum(r.bytes_moved for r in self.rounds)


class RebalancePlanner:
    """根据集群状态生成分阶段 PG 迁移计划。"""

    def plan(self,
             osds: List[OSD],
             pgs: List[PG],
             config: Optional[PlannerConfig] = None) -> Plan:
        cfg = config or PlannerConfig()
        osd_by_id = self._validate(osds, pgs, cfg)

        mapping = {pg.pg_id: list(pg.osds) for pg in pgs}
        pg_by_id = {pg.pg_id: pg for pg in pgs}
        used = {o.osd_id: 0 for o in osds}
        for pg in pgs:
            for oid in pg.osds:
                used[oid] += pg.size

        moves = []          # type: List[Move]
        moved_pgs = set()   # 每个 PG 全计划最多移动一次，杜绝重复移动

        ideals = self._ideal_loads(osds, used)

        def legal_target(pg: PG, dst: int) -> bool:
            osd = osd_by_id[dst]
            if not osd.up or dst in mapping[pg.pg_id]:
                return False
            hosts = {osd_by_id[o].host for o in mapping[pg.pg_id]}
            if osd.host in hosts:
                return False
            if cfg.enforce_rack:
                racks = {osd_by_id[o].rack for o in mapping[pg.pg_id]}
                if osd.rack in racks:
                    return False
            # 目标不能被推过 near-full 阈值
            if used[dst] + pg.size > osd.capacity * cfg.near_full_ratio:
                return False
            return True

        def apply_move(pg: PG, src: int, dst: int) -> None:
            mapping[pg.pg_id].remove(src)
            mapping[pg.pg_id].append(dst)
            used[src] -= pg.size
            used[dst] += pg.size
            moves.append(Move(pg.pg_id, src, dst, pg.size))
            moved_pgs.add(pg.pg_id)

        # ---- 阶段 1：降级 PG 优先（副本位于故障 OSD，或被标记降级） ----
        for pg in sorted(pgs, key=lambda p: p.pg_id):
            broken = [o for o in mapping[pg.pg_id] if not osd_by_id[o].up]
            if not broken and not pg.degraded:
                continue
            if broken:
                src = broken[0]
            else:
                src = max(mapping[pg.pg_id], key=lambda o: used[o])
            targets = [o.osd_id for o in osds if legal_target(pg, o.osd_id)]
            if not targets:
                raise InfeasibleError(
                    "failure-domain",
                    "PG %s 降级，但没有任何目标 OSD 同时满足主机/机架故障域"
                    "隔离与 near-full 容量约束" % pg.pg_id)
            dst = min(targets,
                      key=lambda t: (used[t] - ideals.get(t, 0), used[t], t))
            apply_move(pg, src, dst)

        # ---- 阶段 2/3：near-full 卸载 + 按权重再平衡 ----
        stalled = set()
        while True:
            def overload(o):
                return used[o.osd_id] - ideals[o.osd_id]

            def is_urgent(o):
                return used[o.osd_id] > o.capacity * cfg.near_full_ratio

            candidates = [o for o in osds
                          if o.up and o.osd_id not in stalled]
            urgent = [o for o in candidates if is_urgent(o)]
            over = [o for o in candidates
                    if overload(o) > 0
                    and (ideals[o.osd_id] == 0
                         or overload(o) > ideals[o.osd_id] * cfg.tolerance)]
            pool = urgent or over
            if not pool:
                break
            pool.sort(key=lambda o: (is_urgent(o), overload(o), o.osd_id),
                      reverse=True)

            progress = False
            for src_osd in pool:
                src = src_osd.osd_id
                pg_ids = [pid for pid, owners in mapping.items()
                          if src in owners and pid not in moved_pgs]
                # 大 PG 优先，尽快释放源 OSD 空间
                pg_ids.sort(key=lambda pid: (-pg_by_id[pid].size, pid))
                done = False
                for pid in pg_ids:
                    pg = pg_by_id[pid]
                    targets = [o.osd_id for o in osds
                               if legal_target(pg, o.osd_id)]
                    if not is_urgent(src_osd):
                        # 非紧急情况下，目标迁移后不应超出理想值容忍带
                        targets = [t for t in targets
                                   if used[t] + pg.size
                                   <= ideals[t] * (1 + cfg.tolerance)]
                    if not targets:
                        continue
                    dst = min(targets,
                              key=lambda t: (used[t] - ideals[t], used[t], t))
                    apply_move(pg, src, dst)
                    progress = True
                    done = True
                    break
                if done:
                    break
                stalled.add(src)
            if not progress:
                break

        # ---- 硬约束终检：仍有违反即整体不可行 ----
        for o in osds:
            if o.up and used[o.osd_id] > o.capacity * cfg.near_full_ratio:
                raise InfeasibleError(
                    "capacity",
                    "OSD.%d 使用量 %d 超过 near-full 阈值 %d，且不存在满足"
                    "故障域隔离与目标容量的迁移目标"
                    % (o.osd_id, used[o.osd_id],
                       int(o.capacity * cfg.near_full_ratio)))
        for pg in pgs:
            if any(not osd_by_id[o].up for o in mapping[pg.pg_id]):
                raise InfeasibleError(
                    "failure-domain",
                    "PG %s 的副本仍位于故障 OSD 上，无合法迁移目标" % pg.pg_id)

        rounds = self._stage(moves, osd_by_id, cfg)
        return Plan(rounds=rounds, final_mapping=mapping)

    # ------------------------------------------------------------------
    def _validate(self, osds, pgs, cfg):
        if not osds:
            raise InfeasibleError("input", "集群没有任何 OSD")
        ids = [o.osd_id for o in osds]
        if len(set(ids)) != len(ids):
            raise InfeasibleError("input", "存在重复的 OSD id")
        osd_by_id = {o.osd_id: o for o in osds}
        for o in osds:
            if o.weight < 0:
                raise InfeasibleError("input",
                                      "OSD.%d 权重为负" % o.osd_id)
            if o.capacity <= 0:
                raise InfeasibleError("input",
                                      "OSD.%d 容量必须为正" % o.osd_id)
        up_osds = [o for o in osds if o.up]
        if not up_osds:
            raise InfeasibleError("capacity", "没有可用的（up）OSD")
        if sum(o.weight for o in up_osds) <= 0:
            raise InfeasibleError("capacity", "可用 OSD 权重总和为 0")
        hosts = {o.host for o in up_osds}
        racks = {o.rack for o in up_osds}

        used = {o.osd_id: 0 for o in osds}
        for pg in pgs:
            if pg.size <= 0:
                raise InfeasibleError("input",
                                      "PG %s 大小必须为正" % pg.pg_id)
            if len(set(pg.osds)) != len(pg.osds):
                raise InfeasibleError(
                    "input", "PG %s 副本映射包含重复 OSD" % pg.pg_id)
            for oid in pg.osds:
                if oid not in osd_by_id:
                    raise InfeasibleError(
                        "input",
                        "PG %s 引用了不存在的 OSD.%d" % (pg.pg_id, oid))
                used[oid] += pg.size
            rep = len(pg.osds)
            if rep > len(hosts):
                raise InfeasibleError(
                    "failure-domain",
                    "PG %s 需要 %d 个副本，但仅 %d 个可用主机故障域"
                    % (pg.pg_id, rep, len(hosts)))
            if cfg.enforce_rack and rep > len(racks):
                raise InfeasibleError(
                    "failure-domain",
                    "PG %s 需要 %d 个副本，但仅 %d 个可用机架故障域"
                    % (pg.pg_id, rep, len(racks)))
            pg_hosts = [osd_by_id[o].host for o in pg.osds]
            if len(set(pg_hosts)) != len(pg_hosts):
                raise InfeasibleError(
                    "failure-domain",
                    "PG %s 现有映射已违反主机故障域隔离" % pg.pg_id)
            if cfg.enforce_rack:
                pg_racks = [osd_by_id[o].rack for o in pg.osds]
                if len(set(pg_racks)) != len(pg_racks):
                    raise InfeasibleError(
                        "failure-domain",
                        "PG %s 现有映射已违反机架故障域隔离" % pg.pg_id)
        for o in osds:
            if used[o.osd_id] > o.capacity:
                raise InfeasibleError(
                    "capacity",
                    "OSD.%d 数据量 %d 已超过物理容量 %d"
                    % (o.osd_id, used[o.osd_id], o.capacity))
        return osd_by_id

    def _ideal_loads(self, osds, used):
        total = sum(used.values())
        sum_w = sum(o.weight for o in osds if o.up)
        ideals = {}
        for o in osds:
            if o.up and sum_w > 0:
                ideals[o.osd_id] = total * o.weight / sum_w
            else:
                ideals[o.osd_id] = 0.0
        return ideals

    def _stage(self, moves, osd_by_id, cfg):
        """把迁移按优先级顺序装入各轮，遵守带宽与并发约束。"""
        for m in moves:
            if m.size > cfg.recovery_budget:
                raise InfeasibleError(
                    "recovery-budget",
                    "PG %s 大小 %d 超过单轮恢复带宽预算 %d，无法调度"
                    % (m.pg_id, m.size, cfg.recovery_budget))
        rounds = []
        pending = list(moves)
        while pending:
            round_moves = []
            bytes_used = 0
            per_source = Counter()
            per_rack = Counter()
            remaining = []
            for m in pending:
                src_rack = osd_by_id[m.src].rack
                dst_rack = osd_by_id[m.dst].rack
                if (len(round_moves) < cfg.max_parallel
                        and bytes_used + m.size <= cfg.recovery_budget
                        and per_source[m.src] < cfg.max_per_source
                        and per_rack[src_rack] < cfg.max_per_rack
                        and per_rack[dst_rack] < cfg.max_per_rack):
                    round_moves.append(m)
                    bytes_used += m.size
                    per_source[m.src] += 1
                    per_rack[src_rack] += 1
                    per_rack[dst_rack] += 1
                else:
                    remaining.append(m)
            if not round_moves:
                smallest = min(pending, key=lambda m: m.size)
                raise InfeasibleError(
                    "recovery-budget",
                    "PG %s（%d 字节）在恢复预算 %d / 并发限制下无法调度"
                    % (smallest.pg_id, smallest.size, cfg.recovery_budget))
            rounds.append(Round(index=len(rounds), moves=round_moves))
            pending = remaining
        return rounds
