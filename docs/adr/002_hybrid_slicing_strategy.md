# 002. 采用混合切片策略 (Hybrid Slicing Strategy)

**Date:** 2025-12-10  
**Status:** Accepted

## Context (背景)

在 Port Quality Box (Cactus 数据质量检测) 项目中，我们需要对比 nGen (Source A, 基准) 和 Cactus (Source B, 待测) 的数据一致性。

最初，我们采用了**基于业务日期 (Business Date)** 的切片策略：
1.  Scanner 扫描 nGen 的 `On_Chasis_Datetime` 字段，找出新增的日期。
2.  Worker 根据日期过滤数据：`WHERE date = '2023-11-01'`。
3.  对于大数据量日期，采用 ID 范围分片 (Sharding) 进行并行处理，但 SQL 依然叠加日期过滤：`WHERE id BETWEEN x AND y AND date = '...'`。

**问题 (The Problem)**：
在实际运行中，我们发现 nGen 系统的数据导入存在**“物理批次连续但业务日期乱序”**的现象。运维人员可能一次性导入了包含过去 7 天数据的 Excel 文件。这批数据的 ID 是连续递增的（例如 10000~10500），但 `On_Chasis_Datetime` 散落在过去一周。

如果继续使用“ID 分片 + 强制日期过滤”策略：
*   Worker 处理 ID 范围 [10000, 10500] 时，如果附加了 `AND date = '今天'`，那么这批数据中属于“昨天”和“前天”的记录就会被**无声无息地过滤掉 (False Negatives)**，导致漏测。
*   如果仅依赖 Scanner 的日期扫描，`SELECT DISTINCT date` 在千万级数据表上的性能极差 (Full Table Scan)，且难以捕捉到“几分钟前刚插入的乱序数据”。

## Decision (决策)

我们决定采用**基于数据生命周期的混合切片策略 (Lifecycle-based Hybrid Slicing Strategy)**，根据数据是“新插入”还是“被更新”，分别采用不同的切分逻辑。

### 1. 新数据 (Inserts) -> 物理 ID 分片 (Physical ID Slicing)
*   **触发源**：nGen 表的 `MAX(id)` 增长。
*   **适用场景**：新数据导入、日常新增业务流。
*   **策略**：
    *   Scanner 仅监控 `id` 水位线。
    *   Worker 接收 `start_id` 和 `end_id`。
    *   **LogicRunner 执行查询时，仅使用 `WHERE id BETWEEN ...`，严禁叠加日期过滤条件。**
*   **优势**：
    *   **完整性**：无论该批次内数据的业务日期多乱，只要 ID 在范围内，都会被拉取并检测。
    *   **性能**：利用主键索引进行范围扫描，数据库 I/O 效率最高。
    *   **对账逻辑适配**：LogicRunner 会自动计算这批 ID 对应的动态时间窗口 (Dynamic Window)，从而精准拉取 Cactus 侧的对应数据，无需担心跨天问题。

### 2. 旧数据 (Updates) -> 业务日期分片 (Logical Date Slicing)
*   **触发源**：Cactus 表的 `MAX(last_modified_timestamp)` 变更。
*   **适用场景**：存量数据修正、ETL 逻辑重跑、迟到的数据更新。
*   **策略**：
    *   Scanner 监控时间戳水位，反查受影响数据的 `_time_begin` (业务日期)。
    *   Worker 接收 `date_filter`。
    *   LogicRunner 执行查询时，使用 `WHERE date = '...'` 进行全量回扫。
*   **优势**：
    *   **覆盖离散更新**：旧数据更新通常是 ID 离散的，无法用 ID 范围圈定，按日期回扫是唯一可行的兜底方案。

## Consequences (后果)

### Positive (收益)
*   **零漏测**：彻底解决了运维乱序导入导致的漏测问题。
*   **高性能扫描**：Scanner 扫描 `MAX(id)` 的复杂度为 O(1)，相比原来的 `SELECT DISTINCT DATE` (O(N)) 有质的飞跃。
*   **任务初始化更快**：Worker 在 ID 模式下无需先执行 `SELECT MIN(id), MAX(id)` 来确定边界，直接开跑。

### Negative (成本/风险)
*   **代码复杂度增加**：`scanner_universal_dag` 和 `logic_runner` 需要同时维护两套逻辑路径 (`if id_range: ... else: ...`)。
*   **Worker 幂等性依赖**：ID 分片模式下，如果同一个 ID 被多次扫描（如重置水位线），会重复产生报告。需依赖下游的报告聚合层来去重或抑制。

## Implementation Status (实施状态)
*   [x] LogicRunner 已重构，支持纯 ID 模式。
*   [x] Scanner DAG 已重构，支持双向水位检测。
*   [x] Worker DAG 已适配 `mode='id_range'` 参数。

