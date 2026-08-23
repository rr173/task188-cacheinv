# task188-cacheinv — 分布式缓存失效收敛验证服务

本服务让分布式系统工程师登记**键空间、缓存副本与失效协议参数**，提交**源更新与消息投递序列**（有序/乱序/重复/租约/确认/重试），由回放引擎模拟协议执行，验证**单调版本、幂等处理与最终收敛**，输出违反步骤或收敛证明；通过的协议场景可**冻结为回归规格**。

## 标准命令

```bash
# 构建（CGO 关闭，纯 Go SQLite 驱动）
CGO_ENABLED=0 GOTOOLCHAIN=local go build ./...

# 静态检查与测试
CGO_ENABLED=0 GOTOOLCHAIN=local go vet ./...
CGO_ENABLED=0 GOTOOLCHAIN=local go test ./...

# 离线自检（建库→回放→收敛→重开验证恢复），退出码 0 表示通过
go run ./cmd/cacheinv --smoke-test --db /tmp/cacheinv.db
```

## 服务启动

```bash
go run ./cmd/cacheinv --addr :8080 --db cacheinv.db
```

## API 入口（前缀 /api）

| 能力 | 入口 |
| --- | --- |
| 创建/列出/查询键空间 | `POST/GET /api/keyspaces`、`GET /api/keyspaces/{id}` |
| 注册/列出副本、调整状态 | `POST /api/keyspaces/{id}/replicas`、`GET ...`、`POST /api/replicas/{id}/status` |
| 协议参数 | `GET/PUT /api/keyspaces/{id}/protocol` |
| 源版本 | `POST/GET /api/keyspaces/{id}/versions` |
| 场景与消息 | `POST/GET /api/scenarios`、`GET /api/scenarios/{id}`、`POST/GET /api/scenarios/{id}/messages` |
| 回放与状态 | `POST/GET /api/scenarios/{id}/replay` |
| 违反步骤/收敛证明 | `GET /api/scenarios/{id}/violations`、`GET /api/scenarios/{id}/proof` |
| 规格冻结与回归 | `POST /api/scenarios/{id}/specs`、`GET /api/specs`、`GET /api/specs/{id}`、`POST /api/specs/{id}/recheck` |
| 统计与健康 | `GET /api/stats`、`GET /api/health` |

## 核心不变量

- 源端版本**单调递增**，提交倒退版本被拒绝（409）。
- 副本观察版本**不得回滚**：乱序失效先到时保留较新版本，补齐后收敛。
- 同一消息 ID 内容必须一致（幂等）；不一致即违反。
- ACK 必须持有有效租约且来自已知副本；重试受协议上限约束。
- 相同场景指纹复用结果；冻结规格保存完整消息序列哈希，参数变更创建新规格。
