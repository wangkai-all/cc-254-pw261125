# 采集核验工作台（Pointwise Delivery）

本地来源偶尔返回**少行或旧缓存，但 HTTP 状态码仍为 200**。本项目实现一个从零搭建的
采集工作台：分页采集之外，另用**区间数量**与**内容摘要**两个独立接口核验，发现疑点后
只在限定范围内补抓；页面严格区分「已证明完整 / 抽检未见异常 / 确认缺口」三类结论，
并把准确性与请求成本的取舍显式呈现。

纯 Python 标准库实现，无第三方依赖，Python ≥ 3.9。

## 1. 背景问题与设计原则

| 问题 | 对应设计 |
| --- | --- |
| 状态码成功不代表内容正确（少行、旧缓存） | 不信任 `/rows`，用权威 `/count` + `/digest` 交叉核验 |
| 抽检通过不能宣称全量完整 | 证明分 `COMPLETE` / `SAMPLED` / `GAP` 三类，页面分区展示，措辞不可互替 |
| 不能发现一次疑点就全表重抓 | 补抓只重抓**可疑区间覆盖的页**；每区间补抓轮数、全局补抓预算、可疑比例熔断三重限制 |
| 准确性与请求成本需要权衡 | 采集 / 核验 / 补抓三本预算账本，全量与抽检两种模式，请求计数全程可审计 |
| 采集与核验必须基于同一来源版本 | run 创建即**钉选版本**；之后每次权威响应带版本比对，换版即中止并使旧证明失效 |
| 核验途中崩溃要可恢复 | 每页、每区间状态立即落盘 SQLite；进程重启后从检查点续跑，已终态区间不重复计费 |
| 结论需要独立对照 | `baseline.py` 直接读取基准数据文件，不走来源 HTTP，逐区间复核工作台结论 |

## 2. 接口与模块

### 2.1 本地来源服务（`source_server.py`，默认 :8081）

读取接口（题目要求的两个本地读取接口即核验口 count/digest，另加采集口 rows）：

| 接口 | 用途 | 是否可被故障污染 |
| --- | --- | --- |
| `GET /rows?offset=&limit=` | 分页采集，响应带 `version` 字段 | **是**：缺页 / 旧缓存，均返回 200 |
| `GET /count?lo=&hi=` | 区间内行数量（权威） | 否 |
| `GET /digest?lo=&hi=` | 区间内容摘要 + 数量（权威） | 否 |
| `GET /version` | 当前来源版本 | 否 |

管理口（故障注入与换版）：

- `POST /admin/version` `{"version":"v2"}`：立即换版
- `POST /admin/faults` `{"faults":[...]}`：覆盖式故障列表
  - 缺页：`{"type":"drop","offset":400,"limit":50,"ids":[403,407],"once":true}`
    （`ids` 可省，默认丢该页首行；`once` 控制一次性还是持续故障）
  - 旧缓存：`{"type":"stale","offset":800,"limit":50,"once":true}`
    （返回上一版本同偏移页，响应里的 `version` 也会变成旧版本号）
- `POST /admin/drift` `{"after_rows":6,"version":"v2"}`：第 N 次 `/rows` 后自动换版
- `GET /admin/counters` / `POST /admin/reset_counters`：来源侧独立请求计数

### 2.2 摘要协议（`dataset.py`）

每行用 `json.dumps(sort_keys=True, separators=(",",":"))` 规范化为字节，
区间摘要为 `sha256(长度前缀 + 规范化行字节 …)`，行按 id 升序。
来源端 `/digest` 与核验端本地重算共用同一实现，单元测试见 `tests/test_digest.py`。

### 2.3 引擎与存储（`engine.py` / `storage.py`）

- SQLite（WAL）表：`runs`、`ranges`、`collected_rows`、`proofs`、`request_log`、`kv`
- 区间状态机：
  `NEW → COLLECTED →（SUSPECT → REFETCHED）* → PROVEN | SAMPLED_OK | GAP`
- run 状态：`RUNNING / CRASHED / COMPLETE / ABORTED_DRIFT /
  ABORTED_BUDGET / ABORTED_BREAKER / ABORTED_ERROR`
- 预算（默认值）：分页采集 60 次、权威核验 40 次、补抓 20 次；
  每个区间最多补抓 2 轮；可疑/缺口区间超过 50% 熔断

### 2.4 独立基准（`baseline.py`）

直接读取 `ground_truth/v1.json`、`v2.json`，对 run 的每个区间重算数量与摘要，
输出：基准缺口清单、工作台声明是否与基准相符。它不访问来源 HTTP，
因此来源接口本身被污染时，基准结论依然可信。

### 2.5 网页（`web/index.html`，工作台默认 :8080）

- 顶部：来源当前版本、来源侧请求计数
- 控制区：发起 run（全量/抽检、抽检率、三项预算）、恢复/放弃中断 run、基准对照
- 注入区：换版、缺页、旧缓存、途中漂移、计数清零
- 三分区结论卡：**✅ 已证明完整 / ⚠️ 抽检未见异常 / ❌ 确认缺口**
- 区间状态机明细表、证明记录表（`ACTIVE/STALE`）、预算用量条
- 底部固定说明准确性 vs 请求成本的取舍

## 3. 快速开始

```bash
# 方式一：一键启动两个服务（数据与 db 放在 .runtime/）
./scripts/run.sh
# 浏览器打开 http://127.0.0.1:8080

# 方式二：手动启动
python3 -m collect_workbench.init_data --out .runtime/ground_truth
python3 -m collect_workbench.source_server --data-dir .runtime/ground_truth --port 8081
python3 -m collect_workbench.webapp \
  --db .runtime/workbench.db \
  --source http://127.0.0.1:8081 \
  --ground-truth .runtime/ground_truth --port 8080
```

推荐在页面上按顺序体验：

1. 默认全量 run → 6 个区间全部「已证明完整」
2. 切换抽检模式 → 部分区间变为「抽检未见异常」，核验请求数下降
3. 注入一次性缺页（如 offset=400, ids=403,407）→ 触发疑点、限定补抓、复核通过
4. 注入持续缺页 + 调小补抓预算 → 出现「确认缺口」，基准对照确认缺口清单
5. 设置途中漂移 → run 换版中止，旧证明全部 `STALE`
6. 命令行制造崩溃后恢复（见下节）

## 4. 验收演示

```bash
python3 scripts/demo.py        # 8 个场景、41 项断言，全部 PASS 才算通过
python3 -m unittest tests.test_digest -v
```

覆盖场景：

1. 干净来源 + 全量核验 → 全部 COMPLETE，给出成本基线（24 采集 + 13 核验请求）
2. 干净来源 + 50% 抽检 → 出现 SAMPLED，核验请求 13 → 9，且不得出现全量 COMPLETE
3. 一次性缺页 → 疑点 → 仅补抓可疑区间（6 次请求）→ 复核通过，基准零缺口
4. 一次性旧缓存 → 摘要不一致 → 补抓 → 通过
5. 持续缺页 + 补抓预算受限 → 确认 GAP，缺口 id 与独立基准一致，来源侧无全表重抓
6. 采集途中换版 → `ABORTED_DRIFT`，旧证明 `STALE`；换版后新 run 钉选新版本
7. 核验预算耗尽 → `ABORTED_BUDGET`，已取得的 PROVEN 保留，未核验区间不冒认完整
8. 第 8 次请求后模拟进程崩溃（`os._exit(3)`）→ 新进程 `--resume` 续跑完成，
   已终态区间不重复计费，崩溃页按 at-least-once 多重发一次

## 5. 崩溃恢复的命令行用法

```bash
# 终端 A：让引擎在第 8 次来源请求后模拟崩溃（状态已落盘，退出码 3）
python3 -m collect_workbench.engine --db .runtime/wb.db \
  --source http://127.0.0.1:8081 --crash-after 8

# 终端 B：用同一个 db 恢复，run 从检查点继续
python3 -m collect_workbench.engine --db .runtime/wb.db \
  --source http://127.0.0.1:8081 --resume 1
```

网页上检测到 `CRASHED/RUNNING` 的 run 时，会显示「恢复中断 RUN」按钮，
恢复前先向来源确认版本仍是钉选版本；若来源已换版，恢复会以 `ABORTED_DRIFT` 结束。

## 6. 目录结构

```
collect_workbench/
  dataset.py         # 两版基准数据、规范化字节与区间摘要协议、分区
  httpjson.py        # 标准库 HTTP JSON 收发
  source_server.py   # 本地来源：rows/count/digest + 故障/换版管理口
  client.py          # 来源 HTTP 客户端
  storage.py         # SQLite：runs/ranges/rows/proofs/request_log/kv
  engine.py          # 版本钉选、预算账本、核验状态机、限定补抓、熔断、恢复
  baseline.py        # 独立基准（直读基准文件，不走 HTTP）
  webapp.py          # 工作台 HTTP API
  init_data.py       # 生成 ground truth
web/index.html       # 工作台页面（原生 HTML/JS，无构建）
scripts/demo.py      # 端到端 8 场景验收
scripts/run.sh       # 一键启动
tests/test_digest.py # 摘要协议单元测试
```

## 7. 关键语义边界（避免误用）

- `SAMPLED_OK` 只表示「本 run 未对该区间发现异常」，**永远不等于**完整；
  只有 `PROVEN` 区间持有 `COMPLETE` 证明。
- `GAP` 是补抓预算/轮数用尽后仍不一致才下的结论，只针对具体区间，
  不会因为一个区间有问题就重判全表。
- 换版后旧证明标记 `STALE` 而非删除：历史请求流水与证明保留，可审计，
  但不得再作为当前版本的完整性证据。
- 预算耗尽属于**优雅中止**：已完成区间的证明继续有效，未完成区间保持未决状态，
  可以调大预算后恢复续跑。
