# 企业排污许可与超标处置

汇总许可限值、连续在线监测、复测与工况变化，按排放口归并到同一条事件处置链，并跟踪整改、合规复核、主管关闭与历史版本。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、批次判定、连续异常合并、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、兼容迁移、事务、版本控制、批次和审计链。
- `src/service.py`：权限检查、批次归并、严重度重算、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、批次处置和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8313
```

默认端口为`8313`，首次启动自动建库；旧数据库会自动补充排放口、批次、读数和版本表。使用`X-Actor`和`X-Role`请求头传递身份。

## 处置规则

- 新批次必须提供`outlet`；同一排放口存在未关闭事件时自动归入当前事件，否则自动创建事件。
- `batch_ref`全局唯一；重复提交返回`duplicate: true`和首次批次，不再插入读数、记录或提升版本。
- `online`批次按读数时间切分连续异常：连续超限合并为一个`continuous_anomaly`，单次异常保留为`single_fluctuation`。
- 在线读数的`raw_judgment`逐条保留；合并记录保存峰值、比值、起止时间和读数数量。
- `permit`更新许可限值后按新限值重算当前结论；`condition`异常会提升严重度，恢复正常后取消该提升。
- `retest`达标会关闭未完成连续异常；未达标继续保留处置事项并按复测值重算。
- 每次批次重算生成新的`item_versions`，详情接口仅返回当前结论；历史结论、批次和事件仍可查询。
- 操作链为：运行人员整改 → 合规人员复核 → 主管关闭；关闭前必须没有未完成事项。
- 状态流转必须提交`expected_version`。两人同时处理时，旧版本提交返回409，后到的人读取最新版本后重评。

## 主要接口

- `GET /health`
- `GET /api/items`，可带`status`、`outlet`过滤
- `POST /api/items`
- `GET /api/items/{id}`：当前严重度、整改期限、执法升级、依据批次和异常片段
- `POST /api/items/{id}/records`
- `GET /api/items/{id}/records`
- `GET /api/items/{id}/batches`：批次、读数、原始判定和升级后的处置历史
- `GET /api/items/{id}/versions`：每次原始判定与后续重算版本
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/batches`
- `GET /api/audit?entity_id={id}`

允许角色：`operator`、`compliance_officer`、`director`、`viewer`。批次和整改记录可由`operator`、`compliance_officer`提交；审计仅`director`、`viewer`可查。

## 批次示例

```json
{
  "batch_ref": "ONLINE-20260929-001",
  "outlet": "OUTLET-01",
  "batch_type": "online",
  "measured_at": "2026-09-29T08:00:00+00:00",
  "permit_limit": 10,
  "readings": [
    {"measured_at": "2026-09-29T08:00:00+00:00", "value": 12.5},
    {"measured_at": "2026-09-29T08:05:00+00:00", "value": 13.2}
  ]
}
```

`batch_type`支持：

- `permit`：提交`permit_limit`。
- `online`：提交`readings`，可选本次`permit_limit`。
- `retest`：提交`value`，可选本次`permit_limit`。
- `condition`：提交`condition_status=normal|abnormal`，可选`value`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
