# prediction-bridge

电力二次调频预测流水线的「承上启下」后端：

```
processor → [prediction-bridge] → MinIO → 解压落地 → SFP-2 /predict → Markdown → 飞书群
```

## 1. 端到端时序

```
processor
   │  POST /api/v1/notifications/processor
   ▼
prediction-bridge  ── 202 Accepted {trace_id}
   │ (后台任务)
   ├─ pending
   ├─ downloading   ← MinIO SDK (fallback: download_url)
   ├─ extracting    ← 解压 + 原子移动到 <traindata_root>/<data_date>/
   ├─ (optional) POST /datasets/rebuild
   ├─ predicting    ← POST /predict?date=<data_date + 1 day>
   ├─ notifying     ← 渲染 Markdown → 飞书 upload_file + text@all + file
   └─ done | failed
```

注：**落地目录使用数据日期**（`date_range.start`），而**预测参数与 Markdown 文件名使用次日**。

## 2. 快速开始

### 2.1 Docker（推荐）

```bash
# 1) 准备配置
cp config/config.example.yaml config/config.yaml
# 编辑 minio / feishu / storage / predictor 的参数

# 2) 构建镜像
docker compose -f deploy/docker-compose.yaml build

# 3) 启动（注意挂载 traindata 宿主机目录）
docker compose -f deploy/docker-compose.yaml up -d

# 4) 查看日志
docker compose -f deploy/docker-compose.yaml logs -f
```

默认挂载与端口映射：

| 宿主机 | 容器 | 用途 |
|---|---|---|
| `./config/config.yaml` | `/app/config/config.yaml:ro` | 运行配置（其中 `download_dir / output_dir / sqlite_path / logging.dir` 会被 compose 的环境变量覆盖为容器路径） |
| 卷 `prediction_bridge_data` | `/var/lib/prediction-bridge` | 下载缓存、报告、任务 SQLite |
| 卷 `prediction_bridge_logs` | `/var/log/prediction-bridge` | JSON 行日志 |
| `/data/deploy/electricity-prediction/sfp2-deploy/traindata` | 同路径 | SFP-2 预测服务的训练数据目录 |
| 端口 `28042`（宿主对外） | `8042`（容器内部） | 容器内监听 8042，对外映射为 28042 |

敏感字段通过环境变量注入（compose `environment` / `.env`）：

```bash
MINIO__ACCESS_KEY=...
MINIO__SECRET_KEY=...
FEISHU__APP_SECRET=...
```

### 2.2 裸机

```bash
# 运行账户 & 目录（宿主路径与 config.example.yaml 保持一致）
sudo useradd --system --home-dir /opt/prediction-bridge prediction-bridge
sudo mkdir -p /opt/prediction-bridge \
             /data/deploy/prediction-bridge/{downloads,reports} \
             /data/deploy/log/prediction-bridge
sudo chown -R prediction-bridge:prediction-bridge \
    /opt/prediction-bridge \
    /data/deploy/prediction-bridge \
    /data/deploy/log/prediction-bridge

# 部署
sudo -u prediction-bridge git clone <repo> /opt/prediction-bridge
cd /opt/prediction-bridge
sudo -u prediction-bridge python3 -m venv .venv
sudo -u prediction-bridge .venv/bin/pip install -r requirements.txt
sudo -u prediction-bridge cp config/config.example.yaml config/config.yaml
# 编辑 config/config.yaml + 新建 /opt/prediction-bridge/.env

sudo cp deploy/systemd/prediction-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now prediction-bridge
sudo systemctl status prediction-bridge
```

### 2.3 本地开发

```bash
cp config/config.example.yaml config/config.yaml
./scripts/run_dev.sh 28042
```

## 3. 接口

全部接口：`http://<host>:28042/docs`（FastAPI 自动生成）。

### 3.1 回调入口

```bash
curl -X POST http://127.0.0.1:28042/api/v1/notifications/processor \
  -H 'Content-Type: application/json' \
  -d '{
    "categories": ["实时市场出清概况", "日前市场出清概况", "抽蓄电站水位", "断面约束", "机组实际发电曲线"],
    "date_range": {"start": "2026-03-26", "end": "2026-03-26"},
    "object_name": "2026-03-26.tar.gz",
    "md5": "176b87546574fc271a75bb4ea87ae755",
    "download_url": "http://112.126.80.142:29000/sxpx/output/2026-03-26.tar.gz"
  }'
```

立即返回：

```json
{"status": "accepted", "trace_id": "xxx-uuid", "received_at": "2026-04-19T12:00:00Z"}
```

### 3.2 任务状态

```bash
curl http://127.0.0.1:28042/api/v1/tasks/<trace_id>
```

状态流转：`pending → downloading → extracting → predicting → notifying → done`（失败时为 `failed` 并带有 `error` 字段）。

### 3.3 健康检查

```bash
curl http://127.0.0.1:28042/health
```

带 30 秒缓存的 MinIO / Predictor / Feishu 连通性探针。

## 4. 配置项

所有字段都在 `config/config.example.yaml` 里有完整示例；下表仅列关键项。

| 路径 | 默认值 | 说明 |
|---|---|---|
| `app.host` / `app.port` | `0.0.0.0` / `28042` | 监听地址（仅用作记录；实际端口由 `uvicorn --port` 决定：裸机 28042、容器内 8042） |
| `app.api_prefix` | `/api/v1` | REST 前缀 |
| `app.callback_path` | `/notifications/processor` | 回调路径（拼在 `api_prefix` 之后） |
| `minio.endpoint` | — | MinIO 地址（不含 scheme） |
| `minio.bucket` / `minio.object_prefix` | `sxpx` / `output/` | 桶与对象前缀 |
| `minio.download_dir` | `/var/lib/prediction-bridge/downloads` | 本地下载缓存 |
| `minio.md5_retry` | `3` | 下载 + MD5 校验重试次数 |
| `minio.fallback_to_http` | `true` | SDK 不可用时回退到 `download_url` 直链下载 |
| `storage.traindata_root` | — | **必填**，SFP-2 预测服务的 traindata 根 |
| `storage.on_conflict` | `overwrite` | `overwrite` / `skip` |
| `storage.keep_failed_artifacts` | `true` | 失败时保留临时文件便于排查 |
| `predictor.base_url` | — | **必填**，SFP-2 base URL |
| `predictor.rebuild_dataset_before_predict` | `false` | 预测前是否先触发 `/datasets/rebuild` |
| `predictor.retry` / `retry_interval_sec` | `3` / `10` | 指数退避（首次间隔、最长 8×） |
| `report.output_dir` | — | Markdown 报告输出目录 |
| `report.template_path` | `app/templates/prediction.md.j2` | Jinja2 模板路径 |
| `feishu.enabled` | `true` | 关闭时跳过飞书通知 |
| `feishu.targets[].mention_all` | `true` | 默认 `@所有人`；false 时使用 `mention_ids/names` |
| `feishu.alert_on_failure` | `true` | 失败时给首个群发告警 |
| `task_store.backend` | `in_memory` | `in_memory` / `sqlite` |
| `task_store.dedup_ttl_sec` | `900` | 同 (object_name,md5) 的去重窗口 |
| `concurrency.mode` | `serial` | `serial` / `thread_pool` |
| `logging.level` | `INFO` | 日志级别 |
| `logging.json` | `true` | JSON 行日志 |

凭证走环境变量覆盖：

```bash
MINIO__ACCESS_KEY=AKxxxx
MINIO__SECRET_KEY=SKxxxx
FEISHU__APP_ID=cli_xxxx
FEISHU__APP_SECRET=xxxx
CALLBACK__SECRET=xxxx
```

YAML 里这些字段保持空即可；生产中不要把密钥写入 YAML。

## 5. 日志

- 位置：`logging.dir`（裸机默认 `/data/deploy/log/prediction-bridge/prediction-bridge.log`；容器内 `/var/log/prediction-bridge/prediction-bridge.log`）+ stderr。
- 格式：JSON 行（`logging.json=true`）。
- 每行包含 `timestamp / level / message / trace_id / stage / extra`。
- 敏感字段（`app_secret / access_key / secret_key / token`）在日志里自动脱敏。

常用 grep：

```bash
# 查看一个 trace 的完整链路（裸机）
grep '"trace_id":"<uuid>"' /data/deploy/log/prediction-bridge/prediction-bridge.log

# 仅看失败
grep '"stage":"predict"' /data/deploy/log/prediction-bridge/prediction-bridge.log | jq 'select(.level=="ERROR")'
```

## 6. 测试

```bash
pip install -r requirements.txt
pytest -q
```

- `tests/test_archive.py` — 解压、防路径穿越、覆盖/跳过冲突
- `tests/test_predictor_client.py` — 200/400/503/超时 的行为（`respx`）
- `tests/test_report_renderer.py` — 模板渲染内容
- `tests/test_feishu_client.py` — token / upload / send / @all
- `tests/test_notifications_api.py` — `TestClient` 端到端，外部依赖全部 mock

离线 smoke：

```bash
python scripts/smoke_pipeline.py
# 或指定一个真实的 tar.gz
python scripts/smoke_pipeline.py --archive /path/to/2026-03-26.tar.gz
```

## 7. 从 0 到 1 启动清单

1. `cp config/config.example.yaml config/config.yaml`，填入 MinIO / 预测服务 / 飞书实参。
2. 创建目录并授权（裸机/systemd；容器不需要，Dockerfile 已处理）：
   ```bash
   sudo mkdir -p /data/deploy/prediction-bridge/{downloads,reports} \
                /data/deploy/log/prediction-bridge \
                /data/deploy/electricity-prediction/sfp2-deploy/traindata
   sudo chown -R prediction-bridge:prediction-bridge \
       /data/deploy/prediction-bridge /data/deploy/log/prediction-bridge
   ```
3. 启动（选一）：
   - 容器：`docker compose -f deploy/docker-compose.yaml up -d`（对外暴露 28042）
   - 裸机：`./scripts/run_dev.sh 28042`
   - 生产：`systemctl enable --now prediction-bridge`
4. 冒烟测试回调接口（见 3.1 的 curl）。
5. 查询状态：`curl http://127.0.0.1:28042/api/v1/tasks/<trace_id>`。
6. 预期产物：
   - `<traindata_root>/2026-03-26/`（数据日期）
   - `<report.output_dir>/prediction_2026-03-27.md`（预测目标日）
   - 飞书群中出现 `<at user_id="all">所有人</at>` 文本 + Markdown 文件

## 8. 排障

| 现象 | 排查 |
|---|---|
| 启动直接失败，日志提示「Invalid configuration」 | 检查 `storage.traindata_root` / `predictor.base_url` / `feishu.app_id/secret/targets` |
| 任务长期停在 `downloading` | MinIO 连不上；`/health` 的 `minio` 组件会同步失败；可把 `minio.fallback_to_http: true` 确认下直链可达 |
| MD5 校验失败 | 上游产物还没写完就触发了通知；`md5_retry` / `md5_retry_interval_sec` 覆盖偶发场景；持续失败说明上下游 md5 不一致 |
| traindata 权限不足 | 容器/systemd 运行账户对 `storage.traindata_root` 没有写权限 |
| 预测返回 503 | 预测服务启动中，服务内部已按 `predictor.retry` 自动退避重试 |
| 飞书 `@` 没生效、文本带 `<at user_id=...>` 字面量 | 机器人未加入群 或 缺少「@所有人」权限；见 demo/README_feishu_demo.md 第 8 节 |

## 9. 边界与约束

- 本服务**不鉴权**（`processor` 走内网）；`config.callback.secret` 字段已预留，后续补中间件。
- 任务状态默认进程内 dict；需跨重启持久化时 `task_store.backend: sqlite`。
- 重名密钥：YAML 只放占位，真实值由环境变量或秘密管理注入。
- `object_name` / `date_range` / `md5` / `download_url` 字段名与上游严格一致，不得改名。

## 10. 目录

```
prediction-bridge/
├── app/                 # FastAPI 代码
│   ├── api/             # HTTP 路由
│   ├── core/            # 配置、日志、异常
│   ├── services/        # MinIO / 解压 / 预测 / 渲染 / 飞书 / 编排
│   ├── models/          # Pydantic schema、任务存储
│   └── templates/       # Jinja2 模板
├── config/              # YAML 配置（example + 实际）
├── deploy/              # Dockerfile + compose + systemd
├── scripts/             # run_dev.sh + smoke
└── tests/               # pytest 套件
```
