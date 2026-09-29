# 企业排污许可与超标处置

把许可限值、连续在线监测数据、工况变化、复测纳入同一条处置链：按排放口归集事件、批次去重、连续异常合并而单次波动保留，工况/复测到达后重算严重度、整改期限与执法升级，并完整保留每次原始判定。

## 处置链规则

- **许可限值按排放口管理**：注册排放口时给出污染物与许可限值，所有判定以该限值为基准（`value > limit` 判异，等于限值不算超标）。
- **新批次归当前事件**：批次按排放口归入该排放口最近一个未关闭事件；没有则自动开立。事件关闭后再来批次另开新事件。
- **重复批次只记一次**：`batch_ref` 全局唯一，重复提交返回 `duplicated=true`，不再产生批次、发现或新判定版本。
- **连续异常合并，单次波动留下**：在线读数按时间排序，相邻异常段长度 ≥2 合并为一条 `continuous` 发现（记峰值、起止、点数），孤立单点保留为 `fluctuation`。波动只升到 `watch`，不按连续超标处理。
- **重算（工况或复测到达触发）**：汇总事件全部未解除发现 + 最新工况 + 最新复测，重算：
  - 严重度 `normal/watch/exceedance/major`；连续异常峰值≥2倍限值、两段以上连续异常、复测仍超标且此前已超标、超标叠加工况异常 → `major`。
  - 整改期限（按严重度与超标倍数收紧：72/24/8/4 小时）。
  - 执法升级阶梯 `none → notice → rectification_order → penalty`；复测仍超标再升一级。
  - 复测合格则此前在线异常解除（曾超标的保留 `watch` 待主管关闭）。
- **每次原始判定都保留**：每次重算向 `judgments` 追加一个不可变版本（v1 为开立基线）。事件详情只返回 `current_judgment`，历史走 `/judgments`。
- **处置分工**：运行人员整改（`remediation`）、合规复核（`assessing`/`inspection`）、主管关闭（`closed`）。关闭前不得有未关闭整改项，且当前结论不得仍有未解除超标。
- **两人同时处理**：所有流转必须带 `expected_version`；批次到达或状态流转都会推进版本，后到的人拿旧版本得到 409，须按最新版本重评后重试。
- **旧数据升级**：打开旧版 SQLite 库时自动迁移（`PRAGMA user_version=2`），补排放口列、`LEGACY` 排放口与 v1 回填判定；旧的批次、事件、整改记录与审计历史继续可查，旧事件仍可接收新批次。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：在线读数合并/波动分类、判定重算、状态机、角色矩阵、期限与关闭不变量。
- `src/repository.py`：SQLite建表、旧库迁移、事务、版本控制、批次/发现/判定快照和审计链。
- `src/service.py`：权限检查、批次归集与去重、判定编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件（哈希链）。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败场景、处置链与升级迁移测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8313
```

默认端口为`8313`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- 排放口：`POST /api/outlets`、`GET /api/outlets`
- 监测批次：`POST /api/batches`、`GET /api/batches`
- 事件：`GET /api/items`、`POST /api/items`、`GET /api/items/{id}`（详情含 `current_judgment`）
- 事件子资源：
  - `POST/GET /api/items/{id}/records`（整改项）
  - `GET /api/items/{id}/batches`（批次历史）
  - `GET /api/items/{id}/findings`（连续/波动发现）
  - `GET /api/items/{id}/judgments`（每次原始判定版本）
  - `POST /api/items/{id}/recompute`（手工触发重算，追加判定版本）
  - `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit[?item_id=]`（director/viewer）

批次报文示例：

```json
{"outlet_code":"DW001","batch_ref":"B-20260929-01","kind":"online",
 "data":{"readings":[{"ts":"2026-09-29T09:00Z","value":11.3}]}}
{"outlet_code":"DW001","batch_ref":"C-01","kind":"condition","data":{"status":"abnormal"}}
{"outlet_code":"DW001","batch_ref":"R-01","kind":"retest","data":{"value":6.2}}
```

允许角色：operator, compliance_officer, director, viewer。批次提交与排放口注册限 operator/compliance_officer；状态流转角色：`assessing`/`inspection` 为 compliance_officer，`remediation` 为 operator，`closed` 为 director。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
