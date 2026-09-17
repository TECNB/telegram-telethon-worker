# Telegram Telethon Worker

一个最小 HTTP Worker：从来源频道首次运行时的 Telegram 未读位置开始，将消息组织成资源，按频道历史 Forward Rate 的分位数筛选，再将命中的资源原样转发给指定目标 Bot。它不下载媒体。追更、历史补档和手动转发同步共用 SQLite 投递账本，避免同一来源 group 重复发送给同一目标。

## 1. 准备配置

可以使用内置向导自动打开官方页面并生成 Worker Token：

```bash
.venv/bin/telegram-worker setup
```

向导会打开 <https://my.telegram.org/apps>。创建应用后，只需粘贴 Telegram 显示的 `api_id` 和 `api_hash`；其余配置会自动写入 `.env`。这两项无法由 Worker 自动注册，因为 Telegram 要求你本人完成账号验证。

代理默认留空并直接连接 Telegram。需要经过 Clash 等 SOCKS5 代理时，在 `setup` 中填写代理地址，或之后修改 `TG_PROXY_HOST` 和 `TG_PROXY_PORT`。

`serve` 默认输出中文 `INFO` 级别测试日志，包括连接状态、请求参数、逐条消息的相册 ID、Caption/文字、媒体类型、原文件名和易读时长、匹配/转发结果、已读位置与批次汇总。文字会合并成单行并最多显示 160 个字符。相册中的媒体自身没有文字时，Worker 会读取同组最多 10 个媒体，从其他相册消息获取 Caption，并在日志中注明 Caption 所在的消息 ID。需要查看每条游标写入时，在 `.env` 增加 `WORKER_LOG_LEVEL=DEBUG`。日志不会输出 API Hash、Worker Token 或 Telegram Session。

本机启动时建议保持 `WORKER_HOST=127.0.0.1`。Compose 已自动把容器内的监听地址、Session 和状态路径设为 `0.0.0.0`、`/app/data/telegram` 与 `/app/data/state.json`。

代理是可选项。执行 `setup` 时留空表示直连。注意 Docker 容器中的 `127.0.0.1` 是容器自身；需要使用宿主机代理时，请按部署环境配置 `host.docker.internal` 或 Docker `host-gateway`。

## 2. 首次登录

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/telegram-worker login
```

登录过程会询问手机号、Telegram 验证码，并在开启两步验证时询问密码。Session 文件保存在 `TG_SESSION_PATH`，不要提交或分享它。

Docker 部署时，先用同一份 `./data` 卷完成一次交互登录：

```bash
docker compose run --rm telegram-worker telegram-worker login
docker compose up -d
```

## 3. 启动与调用

```bash
.venv/bin/telegram-worker serve
```

```bash
curl -X POST http://127.0.0.1:8081/forward-unread \
  -H "Authorization: Bearer $WORKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "@ciyuanb",
    "target": "@PikPak_Bot",
    "minVideoDuration": 300,
    "percentile": 0.90,
    "baselineSize": 100,
    "minViews": 5000,
    "minForwards": 10,
    "minAgeHours": 24,
    "resourceMode": "group",
    "markRead": false
  }'
```

首次请求读取来源频道的 `read_inbox_max_id` 作为起点。之后使用 `data/state.json` 中自己的游标，因此不受你随后在 Telegram 客户端中阅读频道的影响。省略 `maxMessages` 时会扫描游标后的全部消息；也可传入 `1` 到 `5000` 限制单次扫描量。

无公开 username 的私有频道可以直接传数字 ID，例如 `"source": -1001735089356`；不要给数字加引号。

默认规则：

- 普通频道以 Telegram album/group（单条消息也算一个 group）作为资源；
- 一个资源只要至少有一个视频严格超过 5 分钟，就通过视频硬条件；
- 仅用达到最低浏览数、最低转发数且已发布至少 24 小时的长视频计算 `forwards / views`；
- 资源内有多个合格视频时取最高 Forward Rate；
- 从 Worker 游标之前开始，每次向历史读取 200 条消息；理想收集 100 个有效资源计算 P90；
- 最多先扫描 2,000 条；若已有至少 50 个有效样本就停止，否则继续到 50 个样本或 5,000 条硬上限；
- 少于 50 个有效历史资源时不自动转发，而是返回“历史有效资源不足”；
- 命中资源按消息时间顺序串行转发，group 内默认间隔 1 秒，资源间默认间隔 3 秒；
- `maxResources` 可选；不传则转发所有达到 P90 的资源。

基线与当前未读候选严格分开，当前候选不会反过来影响自己的 P90。计算结果在 Worker 进程内缓存 24 小时；重启 Worker 后会重新计算。

`minVideoDuration: 300` 使用严格大于，表示视频必须超过 5 分钟。候选也必须达到 `minAgeHours`，避免互动尚未稳定的新消息参与决策。未达到观察时间的资源会保留到下次运行，Worker 游标不会越过它们。

如果符合条件的消息转发失败，本次请求会失败，并且游标不会越过该消息，下次调用会重试。成功转发后会立即保存游标，再记录目标消息 ID。若 Telegram 已接收消息、但网络在返回响应前中断，仍可能重复转发一次，这是当前 MVP 的 at-least-once 语义。

来源频道禁止转发时，Telegram 会拒绝请求，本项目不会绕过限制。

## 先预演，不发送

先启动（已有服务需要重启以加载新参数）：

```bash
cd /Users/tengenchang/Desktop/telegram-telethon-worker
.venv/bin/telegram-worker serve
```

`dryRun` 会返回阈值和候选资源，但不会发送消息、推进 Worker 游标或改变 Telegram 已读位置：

```bash
cd /Users/tengenchang/Desktop/telegram-telethon-worker
set -a
source .env
set +a
curl --fail-with-body -X POST http://127.0.0.1:8081/forward-unread \
  -H "Authorization: Bearer $WORKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"source":"@ciyuanb","target":"@PikPak_Bot","dryRun":true}'
```

确认 `selectedResources` 后，去掉 `dryRun` 或改为 `false` 即可真实执行。

`markRead` 必须为 JSON 布尔值，默认 `false`。为 `true` 时，本批扫描及转发全部成功后才标记到最后扫描的资源（包括被筛掉的内容），不是频道最新消息或最后转发的视频。空批次不标记。响应 `markedReadThrough` 为已标记的消息 ID，未标记时为 `null`。

转发前日志会显示 Telegram 实际解析到的目标名称、用户名和 ID；转发成功后会显示目标会话生成的消息 ID。响应中的 `forwardResults` 同时返回来源消息 ID 与目标消息 ID，方便确认内容实际进入了哪个会话。

`resourceMode: "group"` 会原样转发命中资源的整个 group。

`resourceMode: "hashtag_resource"` 用于果次元这类特殊来源：

- 带 hashtag 的 group 是说明锚点，不发送锚点媒体；
- 后续直到下一个锚点之前的 group 组成一个逻辑资源；
- Worker 会向游标之前寻找最近锚点，避免未读位置落在资源中间时漏掉正文；
- 正文决定是否至少存在一个超过 5 分钟的视频，Forward Rate 使用锚点；
- 每个原始 group 通过 Telegram 服务器端复制发送，并把同一锚点全文作为 album Caption；
- 媒体不下载到 Worker，也不显示 `Forwarded from`；
- 每成功复制一个 group 就写入 `data/state.progress.json`，中断重试时跳过已经完成的 group；整个资源完成后才推进主游标并清除其进度。
- 每个成功发送的原始 group 同时永久写入 `data/state.db`；即使之后重新回溯相同窗口，也不会再次发送。

果次元当前建议使用 `minViews: 500`、`minForwards: 1`；普通频道仍使用默认的 `5000/10`。

## 历史补档

`POST /backfill` 使用独立历史游标，不读取或修改 Telegram 未读位置，也不影响 `/forward-unread` 的正向游标。`startMode: "continue"` 从上次窗口继续向前；`startMode: "latest"` 从最新位置重新计算相同近期范围。

默认规则是最近半年内满足硬条件的资源按 Forward Rate 全局排序，取 Top 10；最多读取 5,000 条消息：

```bash
curl --fail-with-body -X POST http://127.0.0.1:8081/backfill \
  -H "Authorization: Bearer $WORKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "@cutexf1v1",
    "target": "@PikPak_Bot",
    "lookbackDays": 180,
    "topResources": 10,
    "maxMessages": 5000,
    "startMode": "continue",
    "dryRun": true
  }'
```

`dryRun` 返回排名但不发送、不推进历史游标。真实执行只有在整个 Top N 批次成功后才将独立游标写入 `data/state.backfill.json`；`startMode: "latest"` 不改变继续回溯游标。

如果先保存 Top 10，之后希望扩大到同一近期范围的 Top 20，使用 `startMode: "latest"` 和 `topResources: 20`。Worker 会重新计算 Top 20，通过永久投递账本跳过原来的前 10，只发送新增资源。返回中的 `selectedResourceCount`、`duplicateResourceCount` 和 `forwardedResourceCount` 会分别给出入选、重复和本次新增数量。

## 频道解析与手动转发同步

Java 或其他调用方可以先解析并验证频道：

```bash
curl --fail-with-body -X POST http://127.0.0.1:8081/resolve-source \
  -H "Authorization: Bearer $WORKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"source":"@ciyuanb"}'
```

如果曾通过 Telegram 官方客户端把频道消息原生转发给目标 Bot，可以扫描目标会话的转发来源并写入同一投递账本：

```bash
curl --fail-with-body -X POST http://127.0.0.1:8081/reconcile-target \
  -H "Authorization: Bearer $WORKER_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"target":"@PikPak_Bot","maxMessages":10000}'
```

首次可以扫描较多历史，后续调用只读取上次同步位置之后的新消息。只有保留 Telegram 原始转发来源的消息可以自动识别；重新上传或复制后失去来源信息的媒体无法可靠匹配。

## 持久化数据

`data/` 中需要长期保留：

- `telegram.session`：Telegram 登录 Session；
- `state.json`：追更游标；
- `state.backfill.json`：历史回溯游标；
- `state.reconcile.json`：目标会话同步游标；
- `state.db`：永久投递账本和幂等请求结果；
- `state.progress.json`：旧版本兼容和正在执行资源的临时进度。

请求可以携带稳定的 `requestId`。相同 `requestId` 和操作类型再次调用时，Worker 直接返回第一次成功结果并设置 `replayedResponse: true`，不会重新发送。

转发异常会阻止本批标记已读；已成功处理的扫描游标仍保留。标记已读本身失败也保留扫描游标，下一批非空且成功时可以推进已读位置；空批次不会补标。这里的转发成功仅表示 Telegram 接收转发，不代表 PikPak 已完成保存。
