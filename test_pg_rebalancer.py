"""pg_rebalancer 的单元测试。

运行：python -m unittest
"""

import unittest
from collections import Counter

from pg_rebalancer import (InfeasibleError, OSD, PG, Plan, PlannerConfig,
                           RebalancePlanner)

PG_SIZE = 100
CAPACITY = 2000


def make_osds(count=6, capacity=CAPACITY, weight=1.0):
    """3 个机架，每机架 2 台主机，每台主机 1 个 OSD。"""
    osds = []
    for i in range(count):
        osds.append(OSD(osd_id=i,
                        host="host%d" % i,
                        rack="rack%d" % (i // 2 + 1),
                        weight=weight,
                        capacity=capacity))
    return osds


def balanced_pgs():
    """6 个 PG，副本=2，均匀分布在 6 个 OSD 上（每个 OSD 200 字节）。"""
    pairs = [(0, 3), (1, 4), (2, 5), (0, 5), (1, 3), (2, 4)]
    return [PG(pg_id="pg.%d" % n, size=PG_SIZE, osds=list(pair))
            for n, pair in enumerate(pairs)]


def used_by_osd(plan, osds, pgs):
    sizes = {pg.pg_id: pg.size for pg in pgs}
    used = {o.osd_id: 0 for o in osds}
    for pg_id, owners in plan.final_mapping.items():
        for oid in owners:
            used[oid] += sizes[pg_id]
    return used


class PlanAssertions(object):
    def assert_mapping_valid(self, plan, osds, pgs, cfg):
        osd_by_id = {o.osd_id: o for o in osds}
        sizes = {pg.pg_id: pg.size for pg in pgs}
        for pg_id, owners in plan.final_mapping.items():
            self.assertEqual(len(owners), len(set(owners)),
                             "%s 副本 OSD 重复" % pg_id)
            hosts = [osd_by_id[o].host for o in owners]
            self.assertEqual(len(hosts), len(set(hosts)),
                             "%s 违反主机故障域隔离" % pg_id)
            if cfg.enforce_rack:
                racks = [osd_by_id[o].rack for o in owners]
                self.assertEqual(len(racks), len(set(racks)),
                                 "%s 违反机架故障域隔离" % pg_id)
            for oid in owners:
                self.assertTrue(osd_by_id[oid].up,
                                "%s 副本仍位于故障 OSD.%d" % (pg_id, oid))
        used = used_by_osd(plan, osds, pgs)
        for o in osds:
            if o.up:
                self.assertLessEqual(
                    used[o.osd_id], o.capacity * cfg.near_full_ratio,
                    "OSD.%d 超过 near-full 阈值" % o.osd_id)

    def assert_rounds_valid(self, plan, osds, cfg):
        osd_by_id = {o.osd_id: o for o in osds}
        for rnd in plan.rounds:
            self.assertLessEqual(rnd.bytes_moved, cfg.recovery_budget,
                                 "第 %d 轮超过恢复带宽预算" % rnd.index)
            self.assertLessEqual(len(rnd.moves), cfg.max_parallel,
                                 "第 %d 轮超过并发迁移上限" % rnd.index)
            per_source = Counter(m.src for m in rnd.moves)
            for src, n in per_source.items():
                self.assertLessEqual(n, cfg.max_per_source,
                                     "第 %d 轮源 OSD.%d 并发超限" % (rnd.index, src))
            per_rack = Counter()
            for m in rnd.moves:
                per_rack[osd_by_id[m.src].rack] += 1
                per_rack[osd_by_id[m.dst].rack] += 1
            for rack, n in per_rack.items():
                self.assertLessEqual(n, cfg.max_per_rack,
                                     "第 %d 轮机架 %s 并发超限" % (rnd.index, rack))


class TestExpansionAndWeights(unittest.TestCase, PlanAssertions):
    def test_disk_expansion_rebalance(self):
        """扩容新增 OSD 后，计划把 PG 迁移到新 OSD，且满足全部约束。"""
        osds = make_osds()
        pgs = balanced_pgs()
        # 磁盘扩容：新增 rack4 中的两个空 OSD
        osds.append(OSD(6, "host6", "rack4", 1.0, CAPACITY))
        osds.append(OSD(7, "host7", "rack4", 1.0, CAPACITY))
        cfg = PlannerConfig(recovery_budget=250, max_parallel=2,
                            max_per_source=1, max_per_rack=2)
        plan = RebalancePlanner().plan(osds, pgs, cfg)
        self.assertTrue(plan.moves, "扩容后应产生迁移计划")
        used = used_by_osd(plan, osds, pgs)
        self.assertGreater(used[6] + used[7], 0, "新 OSD 应承接数据")
        self.assert_mapping_valid(plan, osds, pgs, cfg)
        self.assert_rounds_valid(plan, osds, cfg)

    def test_weight_change_shifts_data(self):
        """权重变化后，高权重 OSD 应承接更多数据。"""
        osds = make_osds()
        pgs = balanced_pgs()
        osds[4].weight = 2.0
        osds[5].weight = 2.0
        cfg = PlannerConfig()
        plan = RebalancePlanner().plan(osds, pgs, cfg)
        used = used_by_osd(plan, osds, pgs)
        self.assertGreater(used[4] + used[5], used[0] + used[1],
                           "高权重 OSD 应比低权重 OSD 承载更多数据")
        self.assert_mapping_valid(plan, osds, pgs, cfg)
        self.assert_rounds_valid(plan, osds, cfg)


class TestNodeFailure(unittest.TestCase, PlanAssertions):
    def test_failed_osd_evacuated_first(self):
        """节点故障：故障 OSD 上的降级 PG 优先迁移，最终映射不含故障 OSD。"""
        osds = make_osds()
        pgs = balanced_pgs()  # pg.2=(2,5)、pg.5=(2,4) 位于 osd.2
        osds[2].up = False
        cfg = PlannerConfig(max_parallel=2)
        plan = RebalancePlanner().plan(osds, pgs, cfg)
        for owners in plan.final_mapping.values():
            self.assertNotIn(2, owners)
        first_round_pgs = {m.pg_id for m in plan.rounds[0].moves}
        self.assertEqual({"pg.2", "pg.5"}, first_round_pgs,
                         "降级 PG 应安排在第一轮")
        self.assert_mapping_valid(plan, osds, pgs, cfg)
        self.assert_rounds_valid(plan, osds, cfg)

    def test_degraded_flag_without_down_osd(self):
        """显式标记降级的 PG 也会被优先处理。"""
        osds = make_osds()
        pgs = balanced_pgs()
        pgs[0].degraded = True
        plan = RebalancePlanner().plan(osds, pgs, PlannerConfig())
        self.assertEqual("pg.0", plan.moves[0].pg_id)
        self.assert_mapping_valid(plan, osds, pgs, PlannerConfig())


class TestRecoveryBudget(unittest.TestCase):
    def test_pg_larger_than_budget_is_infeasible(self):
        """单个 PG 超过单轮恢复预算 → 报 recovery-budget 约束。"""
        osds = make_osds()
        pgs = balanced_pgs()
        osds[2].up = False  # 强制产生迁移需求
        cfg = PlannerConfig(recovery_budget=PG_SIZE - 1)
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs, cfg)
        self.assertEqual("recovery-budget", ctx.exception.constraint)


class TestNearFull(unittest.TestCase, PlanAssertions):
    def _crowded_pgs(self):
        pairs = [(0, 2)] * 3 + [(0, 3)] * 2 + [(0, 4)] * 2 + [(0, 5)] * 2
        return [PG(pg_id="pg.%d" % n, size=PG_SIZE, osds=list(pair))
                for n, pair in enumerate(pairs)]

    def test_near_full_source_drained_below_threshold(self):
        """接近满盘的 OSD 被优先卸载到阈值以下，目标不被推过阈值。"""
        osds = make_osds(capacity=1000)
        pgs = self._crowded_pgs()  # osd.0 使用 900 > 850 阈值
        cfg = PlannerConfig(near_full_ratio=0.85)
        plan = RebalancePlanner().plan(osds, pgs, cfg)
        used = used_by_osd(plan, osds, pgs)
        self.assertLessEqual(used[0], 1000 * 0.85)
        self.assert_mapping_valid(plan, osds, pgs, cfg)
        self.assert_rounds_valid(plan, osds, cfg)

    def test_all_targets_near_full_is_infeasible(self):
        """所有目标都接近满盘、无法安全卸载 → 报 capacity 约束。"""
        osds = make_osds(capacity=1000)
        pairs = [(0, 4), (1, 5), (0, 5), (1, 2), (1, 3),
                 (0, 2), (2, 4), (3, 4), (3, 5)]
        pgs = [PG(pg_id="pg.%d" % n, size=PG_SIZE,
                  osds=list(pairs[n % len(pairs)]))
               for n in range(27)]  # 每个 OSD 900 字节，全部越过 850 阈值
        cfg = PlannerConfig(near_full_ratio=0.85)
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs, cfg)
        self.assertEqual("capacity", ctx.exception.constraint)


class TestDuplicateMoves(unittest.TestCase, PlanAssertions):
    def test_no_pg_moved_twice(self):
        """同一 PG 在整个计划中最多迁移一次，且无重复 (PG, 目标)。"""
        osds = make_osds()
        pgs = balanced_pgs()
        osds.append(OSD(6, "host6", "rack4", 1.0, CAPACITY))
        osds.append(OSD(7, "host7", "rack4", 1.0, CAPACITY))
        cfg = PlannerConfig()
        plan = RebalancePlanner().plan(osds, pgs, cfg)
        pg_ids = [m.pg_id for m in plan.moves]
        self.assertEqual(len(pg_ids), len(set(pg_ids)),
                         "同一 PG 被重复迁移")
        pairs = [(m.pg_id, m.dst) for m in plan.moves]
        self.assertEqual(len(pairs), len(set(pairs)),
                         "存在重复的 (PG, 目标) 迁移")
        # 计划应用到初始映射后应与 final_mapping 一致（无来回移动）
        pgsizes = {pg.pg_id: pg.size for pg in pgs}
        self.assertEqual(sum(pgsizes[m.pg_id] for m in plan.moves),
                         plan.total_bytes)


class TestFailureDomainConflicts(unittest.TestCase):
    def test_replicas_exceed_rack_domains(self):
        """副本数超过可用机架数 → 报 failure-domain 约束。"""
        osds = [OSD(0, "h0", "rack1", 1.0, CAPACITY),
                OSD(1, "h1", "rack1", 1.0, CAPACITY),
                OSD(2, "h2", "rack2", 1.0, CAPACITY),
                OSD(3, "h3", "rack2", 1.0, CAPACITY)]
        pgs = [PG("pg.0", PG_SIZE, [0, 2, 3])]  # 3 副本 > 2 机架
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs, PlannerConfig())
        self.assertEqual("failure-domain", ctx.exception.constraint)

    def test_replicas_exceed_host_domains(self):
        """副本数超过可用主机数 → 报 failure-domain 约束。"""
        osds = [OSD(0, "h0", "rack1", 1.0, CAPACITY),
                OSD(1, "h0", "rack2", 1.0, CAPACITY),
                OSD(2, "h1", "rack3", 1.0, CAPACITY)]
        pgs = [PG("pg.0", PG_SIZE, [0, 1, 2])]  # 3 副本 > 2 主机
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs,
                                    PlannerConfig(enforce_rack=False))
        self.assertEqual("failure-domain", ctx.exception.constraint)

    def test_existing_mapping_violates_host_domain(self):
        """现有映射已违反主机隔离 → 拒绝并指明约束，而非输出不安全计划。"""
        osds = [OSD(0, "h0", "rack1", 1.0, CAPACITY),
                OSD(1, "h0", "rack2", 1.0, CAPACITY),
                OSD(2, "h2", "rack3", 1.0, CAPACITY)]
        pgs = [PG("pg.0", PG_SIZE, [0, 1])]  # 两个副本同主机
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs, PlannerConfig())
        self.assertEqual("failure-domain", ctx.exception.constraint)

    def test_degraded_pg_without_legal_target(self):
        """降级 PG 在所有候选目标都违反故障域时 → 报 failure-domain。"""
        osds = make_osds(count=4)  # 2 机架
        for o in osds:
            o.rack = "rack1" if o.osd_id < 2 else "rack2"
        pgs = [PG("pg.0", PG_SIZE, [0, 2])]
        osds[0].up = False
        # 唯一副本在 rack2，候选只剩 rack1 的 osd.1（与 osd.0 同 rack 不算冲突），
        # 但 osd.1 与 osd.0 同在 rack1……这里目标 osd.1 合法，改为全部排除：
        osds[1].up = False
        osds[3].up = False  # 仅剩 osd.2，而副本已在 osd.2 上
        with self.assertRaises(InfeasibleError) as ctx:
            RebalancePlanner().plan(osds, pgs, PlannerConfig())
        self.assertEqual("failure-domain", ctx.exception.constraint)


if __name__ == "__main__":
    unittest.main()
