# task188-cacheinv 评测说明（BENZHI）

## 构建与运行

```bash
# 本机构建门禁
CGO_ENABLED=0 GOTOOLCHAIN=local go build ./...
CGO_ENABLED=0 GOTOOLCHAIN=local go vet ./...
CGO_ENABLED=0 GOTOOLCHAIN=local go test ./...
```

## --smoke-test 契约（Docker 双架构验证判据）

入口 `cmd/cacheinv` 支持 `--smoke-test`：不启动长驻服务，而是真实完成

1. 建库并创建键空间、两个副本；
2. 提交源更新（v3）并验证**版本倒退被拒绝**；
3. 构造乱序失效+租约+确认+更新的消息序列，回放并断言**收敛**；
4. 生成收敛证明并**冻结规格**；
5. 构造无租约 ACK 场景，断言**违反被定位**；
6. **关闭并重开同一数据库**，验证场景/规格/游标恢复；
7. 新场景部分回放后重开续跑，断言**超时未收敛**。

以退出码 0 结束即通过。

```bash
go run ./cmd/cacheinv --smoke-test --db /tmp/cacheinv.db
```

## Docker 双架构

```bash
bash build_benzhi_docker.sh task188-check linux/amd64
bash build_benzhi_docker.sh task188-check linux/arm64
```

## API 一览

- 键空间：`POST/GET /api/keyspaces`、`GET /api/keyspaces/{id}`
- 副本：`POST /api/keyspaces/{id}/replicas`、`GET /api/keyspaces/{id}/replicas`、`POST /api/replicas/{id}/status`
- 协议：`GET/PUT /api/keyspaces/{id}/protocol`
- 源版本：`POST/GET /api/keyspaces/{id}/versions`
- 场景：`POST/GET /api/scenarios`、`GET /api/scenarios/{id}`、`POST/GET /api/scenarios/{id}/messages`
- 回放：`POST/GET /api/scenarios/{id}/replay`
- 证明：`GET /api/scenarios/{id}/violations`、`GET /api/scenarios/{id}/proof`
- 规格：`POST /api/scenarios/{id}/specs`、`GET /api/specs`、`GET /api/specs/{id}`、`POST /api/specs/{id}/recheck`
- 运维：`GET /api/stats`、`GET /api/health`

## 组件版本

- Go 1.26.3（GOTOOLCHAIN=local，CGO_ENABLED=0）
- SQLite 3.46.1（pure-Go 驱动 `modernc.org/sqlite` v1.52.0）
