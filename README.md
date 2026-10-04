# 公共采购密封投标与评审系统

标准库实现的招标发布、密封投标、开标校验收、规则评分、利益冲突、澄清、废标、投诉重评、授标快照和**评标留痕摘要链**服务。

## 运行

要求 Python 3.11+（当前 Python 3.9 环境亦可）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址 `http://127.0.0.1:8209`，数据库默认 `public_procurement.db`。

## 评标留痕摘要链

每次评分连同**采购项目、投标、评分批次（轮次）、评审人与各评分项**写入 `score_chain` 连续摘要链；
利益冲突作废、投诉受理重评、授标冻结同样作为事件节点入链。

- 节点结构：`payload_hash = SHA256(规范化 JSON)`，`node_hash = SHA256(prev_hash | seq | payload_hash)`，
  首节点 `prev_hash` 为 64 个 0。审计人员可逐节点重算验证链完整、定位断点。
- **作废重算**：利益冲突受理后，冲突评审人尚未授标的评分标记 `voided`（原值保留不删），
  排名只统计 `active` 评分，缺分即不能授标；投诉受理（accepted）后当前批次全部评分作废、批次号 +1。
- **授标冻结**：授标时把当前排名写入快照并入链，之后再申报冲突或投诉都不改变已冻结快照。
- **并发 409**：评分与投诉处理基于同一批次快照并发提交时，先拿到写锁提交的一方生效，
  后提交一方返回 409（评分参数 `expected_round`/`expected_invalidation_revision`，
  投诉处理另有 `expected_score_commit_counter`，基线来自 GET 返回的项目状态）。
- **旧数据补链**：旧库首次启动自动迁移，按 `evaluations` 现有主键顺序补建历史节点（`source=backfill`，
  不覆盖任何原值）；找不到投标归属的记录进入 0 号“待核链”并标 `verified=pending`。
- **半截节点恢复**：评分采用「先占位 writing 节点 → 写评分行 → 封口」两阶段。重启时发现 writing 节点：
  评分行齐全则自动 `repaired` 封口，评分行缺失则封成 `aborted` 墓碑（链仍连续），
  只写入一部分则保持 `broken` 待核并**阻断授标**；也可由监管 `POST /api/chain/repair` 触发修复。
- **授标前强制校验**：授标事务内重算整条链，任何篡改或断点一律 409 拒绝。

## 主要接口

使用 `X-User`、`X-Role` 请求头。角色有 `procurement`、`vendor`、`evaluator`、`supervisor`、`auditor`、`public`。

- `GET /health`、`GET /api/state`、`GET /api/tenders/{id}`
- `GET /api/chain?tender_id=`：单项目逐轮校验状态与断点位置（`GET /api/chain` 列全部）
- `POST /api/chain/verify`：重算指定项目链（auditor 可调用）
- `POST /api/chain/repair`：触发半截节点修复（仅 supervisor）
- `POST /api/vendors`、`POST /api/tenders`、`POST /api/tenders/publish`
- `POST /api/bids`、`POST /api/bids/withdraw`、`POST /api/bids/disqualify`
- `POST /api/tenders/open`：截止后开标并核验承诺哈希
- `POST /api/conflicts`、`POST /api/evaluations`
- `POST /api/clarifications`、`POST /api/clarifications/answer`
- `POST /api/complaints`、`POST /api/complaints/resolve`
- `POST /api/tenders/award`：校验摘要链、锁定评分轮次并保存冻结排名快照

公开页面（`/`）展示每个项目每轮链校验状态（完整 / 断裂 / 待核）、节点统计与断点位置。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

- `tests/test_flow.py`：完整开标授标、截止前正文隐藏、利益冲突、重复评分、投诉重评和角色权限。
- `tests/test_chain.py`：链结构与哈希独立验证、作废重算、授标冻结、评分/投诉并发 409（双向确定性交错）、
  旧库补链与待核标记、重启三种半截节点恢复、篡改检测与授标阻断、公开页状态。

## 局限

供应商与请求用户没有绑定校验，身份仍依赖请求头；投标正文虽然按接口阶段隐藏，但数据库本身未加密；评分规则适合演示，不覆盖复杂资格预审、保证金、电子签名和采购法规差异。
摘要链用于发现篡改与过程留痕，节点哈希未做外部签名，可信根仍依赖数据库访问控制。
