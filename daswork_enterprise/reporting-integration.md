# 报表接线与验证

候选在 LiteLLM Proxy 的路由组装阶段调用一次
`build_reporting_router(require_master_key=..., get_query=...)`。三个报表 GET
都使用候选的 Master Key 依赖。缺少凭据时返回 401，普通会话 Key 返回 403。
`get_query` 从请求状态借用 Proxy 已连接的 `writer_db`，复用 `Database.rows` 执行查询，
使结算后的读回能立即看到已提交的行。候选只注册下面
三个 URL 各一次，排除原生同名日表路由。

| 路由 | 读取 |
| --- | --- |
| `/organization/daily/activity/aggregated` | 精确企业、可选诊断排除、UTC 日期范围 |
| `/tag/daily/activity` | 精确 Tag、UTC 日期范围、请求开始时间和主键决定的分页 |
| `/organization/daily/activity/export` | 精确企业、UTC 日期范围、`daily` 或 `daily_with_models` CSV |

候选费用日志保留原生 `/spend/logs/v2`，以候选 Master Key 鉴权保护。列表和执行回读都
携带原生 UTC 起止参数。执行回读可按已哈希的 `api_key`、`model_id`、`model_group`
和 `request_id` 筛选。原生 `request_id` 筛选同时匹配主键和 `litellm_call_id`。

第一批结算写入原生 `LiteLLM_SpendLogs`。`request_id` 是账本主键，`litellm_call_id`
是执行回读关联号。组织、Team、用户、Key 哈希、模型、模型组、供应商和 JSONB 字符串数组
`request_tags` 均来自请求时的可信归属。保留 UTC 开始和结束时间、非负整数时长、原生状态、
三项 token 数、实际费用和失败的 `metadata.error_information.error_code`。
`metadata.usage_object` 保留原生缓存 token 字段，`metadata.status` 与顶层状态一致。
三个报表从已提交账本行读取，不需要日表投影、额外写入、连接或迁移。CSV 的组织别名读自原生
组织表。每个报告请求由一条参数化查询读取。Tag 页的总页数和数据共享同一语句快照，
跨请求翻页会读取各自的已提交数据。

候选应保持原生 CSV 头、UTF-8、CRLF 和公式保护。Java 在下载前使用实际行数和字节数判断
10,000 行与 4 MiB 上限，并再次检查授权及企业。接线验收需实际触发 Java 下载，验证
10,000 行可下载、10,001 行和超过 4 MiB 返回 413 且无附件，以及查询期间撤权或范围变化时
没有附件。报告模块的测试覆盖网关提交给 Java 的原始内容。

在已构建本快照 Rust 扩展并安装 OSS Proxy 测试依赖的候选环境运行以下检查。
`DASWORK_REPORTING_TEST_DSN` 指向可建临时表的测试 PostgreSQL。

```sh
ruff format --check daswork_enterprise/reporting.py daswork_enterprise/reporting_sql.py daswork_enterprise/tests/test_reporting.py
ruff check --config ruff-strict.toml daswork_enterprise/reporting.py daswork_enterprise/reporting_sql.py daswork_enterprise/tests/test_reporting.py
ruff check --config ruff-tests.toml daswork_enterprise/tests/test_reporting.py
python scripts/check_type_discipline.py daswork_enterprise/reporting.py daswork_enterprise/reporting_sql.py daswork_enterprise/tests/test_reporting.py
basedpyright daswork_enterprise/reporting.py daswork_enterprise/reporting_sql.py daswork_enterprise/tests/test_reporting.py
python -m pytest --import-mode=importlib -q daswork_enterprise/tests/test_reporting.py
```

本批在真实 PostgreSQL 17 和 FastAPI 上以合成数据验证读查询、响应和 CSV。临时测试引导程序
绕开 LiteLLM 和仓储包的顶层初始化，替换日志入口，从审查快照加载原始常量、日期验证和 CSV
函数。查询在 psycopg 中执行，鉴权使用注入的测试依赖。候选需在构建后的 Proxy 上复跑，并用
原生 Prisma、Master Key、费用日志和 Java 端到端验证接线。

## PostgreSQL 查询计划

在本机 PostgreSQL 17 的临时表中复现原生 Prisma 的报表列、主键和六个账本索引。生成
720,000 条合成记录，分布在 120 个组织、950 个部门和 180 个 UTC 日期。组织和部门均查询
31 日，取第一个 Tag 页。以下为同一数据库上的单次 `EXPLAIN (ANALYZE, BUFFERS)` 对照，
用时不含 Prisma 和 HTTP。

| 查询 | 原生索引和读取行数 | 临时索引和读取行数 | 耗时对照 |
| --- | --- | --- | --- |
| 组织日用量 | `startTime` B-tree，124,000 行，组织过滤后 1,034 行 | `(organization_id, "startTime")` B-tree，1,034 行 | 119.2 到 14.6 ms |
| 部门 Tag | `startTime` B-tree，124,000 行，Tag 过滤后 156 行 | `GIN (request_tags jsonb_path_ops)`，905 行，日期过滤后 156 行 | 193.4 到 3.9 ms |
| 组织 CSV | `startTime` B-tree，124,000 行，组织过滤后 1,034 行 | `(organization_id, "startTime")` B-tree，1,034 行 | 80.8 到 7.9 ms |

这两个索引分别减少其他组织和其他部门的堆访问，评估候选的实际读延迟时还需量化索引的写入成本。
本批候选沿用原生 schema。上述索引只在临时表对照组创建。
