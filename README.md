# 公共采购密封投标与评审系统

标准库实现的招标发布、密封投标、开标校验收、规则评分、利益冲突、澄清、废标、投诉重评和授标快照服务。

## 运行

要求 Python 3.11+（当前 Python 3.9 环境亦可）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址 `http://127.0.0.1:8209`，数据库默认 `public_procurement.db`。

## 主要接口

使用 `X-User`、`X-Role` 请求头。角色有 `procurement`、`vendor`、`evaluator`、`supervisor`、`auditor`、`public`。

- `GET /health`、`GET /api/state`、`GET /api/tenders/{id}`
- `POST /api/vendors`、`POST /api/tenders`、`POST /api/tenders/publish`
- `POST /api/bids`、`POST /api/bids/withdraw`、`POST /api/bids/disqualify`
- `POST /api/tenders/open`：截止后开标并核验承诺哈希
- `POST /api/conflicts`、`POST /api/evaluations`
- `POST /api/clarifications`、`POST /api/clarifications/answer`
- `POST /api/complaints`、`POST /api/complaints/resolve`
- `POST /api/tenders/award`：锁定评分轮次并保存排名快照
- `GET /api/chain/status`：公开查看评分摘要链每轮校验状态与断点位置
- `POST /api/chain/repair`：supervisor/auditor 修复可修复的链断点

## 评标留痕（评分摘要链）

- 每次评分连同采购项目、投标和评分批次（`batch_id`）写入 `score_chain` 连续摘要链，节点哈希覆盖前序哈希，审计人员可逐节点重算验证链完整。
- 利益冲突申报或投诉受理后：未授标的当前轮评分与排名失效作废（`voided`）并重算，作废事件同样上链；已授标项目的快照保持冻结，不再改动。
- 评分与投诉处理并发提交时，客户端应携带 `expected_round`（评分）/ `expected_version`（投诉处理）：先提交的事务生效，另一方返回 409。
- 旧数据评分没有摘要：服务启动（升级）时按 `evaluations` 主键顺序补链，批次记为 `legacy-<id>`；投标或项目无法确认的记录标记 `pending`（待核），补链只插入链节点，不覆盖原值。
- 服务重启时自检摘要链：上次只写了一半的节点（哈希缺失/断链）自动修复；节点内容残缺无法修复时置为 `blocked` 并阻断授标，人工修复数据后经 `POST /api/chain/repair` 解除。
- 公开首页展示每轮校验状态（节点数、有效/作废/待核、作废事件）和断点位置。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整开标授标、截止前正文隐藏、利益冲突、重复评分覆盖、投诉重评和角色权限；以及评分摘要链写入与篡改定位、冲突/投诉作废重算与授标冻结、评分与投诉处理并发 409、旧数据补链与待核标记、半写节点重启修复和断链阻断授标。

## 局限

供应商与请求用户没有绑定校验，身份仍依赖请求头；投标正文虽然按接口阶段隐藏，但数据库本身未加密；评分规则适合演示，不覆盖复杂资格预审、保证金、电子签名和采购法规差异。
