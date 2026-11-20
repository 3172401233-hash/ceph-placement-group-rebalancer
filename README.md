# ceph-placement-group-rebalancer

规划 Ceph 归置组重平衡、故障域和恢复带宽。

磁盘扩容后归置组（PG）分布严重不均：一次迁移过多 PG 会占满恢复带宽，
移动顺序错误还可能让同一故障域出现过多副本。本项目实现一个 Ceph 风格的
PG 重平衡**规划器/模拟器**（Python 3.8.2+）：根据声明式的集群描述生成
分阶段的 PG 迁移计划。

> **注意：本仓库是纯模拟器。** 它只在内存中根据输入的集群描述计算迁移
> 计划，**不会调用 `ceph`、`ceph osd` 等任何 Ceph 管理命令**，也不连接
> 真实集群。输出的计划需由使用者自行评审后再决定是否执行。

## 输入

- `OSD`：ID、主机、机架、权重、容量（字节）、是否 up（故障为 `False`）
- `PG`：ID、单副本大小、现有 OSD 映射、是否降级
- `PlannerConfig`：
  - `near_full_ratio`：OSD 使用率 near-full 阈值（默认 0.85）
  - `recovery_budget`：单轮恢复带宽预算（字节）
  - `max_parallel`：单轮最大并发迁移数
  - `max_per_source`：单轮同一源 OSD 最大迁移数
  - `max_per_rack`：单轮同一机架（源或目标）最大迁移数
  - `tolerance`：相对理想负载的容忍偏差
  - `enforce_rack`：是否强制机架级故障域隔离

## 行为保证

- 目标分布遵守主机/机架故障域隔离与 near-full 容量约束；
- 单轮迁移字节数不超过恢复带宽预算，且受并发、单源 OSD、单机架限制；
- 降级 PG 与接近满盘的 OSD 优先处理；
- 同一 PG 在整个计划中最多迁移一次，避免重复/来回移动；
- 输入无可行方案时抛出 `InfeasibleError` 并指明具体约束
  （`failure-domain` / `capacity` / `recovery-budget` / `input`），
  绝不输出部分不安全的计划。

## 用法

```python
from pg_rebalancer import OSD, PG, PlannerConfig, RebalancePlanner

osds = [OSD(0, "host0", "rack1", 1.0, 2 << 30), ...]
pgs = [PG("pg.0", 100 << 20, [0, 3]), ...]
cfg = PlannerConfig(recovery_budget=1 << 30, max_parallel=8)

plan = RebalancePlanner().plan(osds, pgs, cfg)
for rnd in plan.rounds:
    for mv in rnd.moves:
        print(rnd.index, mv.pg_id, mv.src, "->", mv.dst, mv.size)
```

## 测试

```bash
python -m unittest
```

覆盖：权重变化、节点故障、恢复预算不足、接近满盘、重复移动、
故障域冲突等场景。
