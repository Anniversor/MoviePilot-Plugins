# qB → 115（MoviePilot V3）

类名 `Qb115Downloader`，版本 `0.1.0`，**默认禁用**。这是独立的虚拟下载器，不是 115 离线下载器，也不替换普通 qB。

## 已实现的链路

```text
明确选择 qB → 115
  → 全种子大小预留 + 独立目录/标签安全检查
  → 现有物理 qB 实例暂停添加、选集、启动
  → 等待 qB 真实选中文件全部完成（priority=0 跳过）
  → 本地逐文件 SHA1 + 固定文件清单
  → OpenList 隔离目录逐文件流式 PUT
  → 大小、可用 SHA1、文件头可读性、整批文件清单验证
  → 单个完整目录 move 到 /115/媒体库监控目录/<原种子名>
  → CLM / CSC / Emby 后续处理；本地原文件继续保种
```

状态包含 `downloading / uploading / verifying / commit_pending / handed_off / waiting_library / failed / suspended / untracked`。页面同时显示真实 qB 进度、qB 状态和云端阶段。**qB 下载 100% 不等于上传完成，更不等于已经入库。**

本插件对 `transfer/可转移` 查询始终返回空，包括指定 hash 的查询，不通过伪造 qB 进度触发 native 整理。`downloader` 必须精确等于 `qB → 115`；其他名称、未指定名称均返回 `None`。

### 与 MoviePilot 的接口契约（已对容器源码逐条核对，见 `tools/check_moviepilot_contract.py`）

- **状态别名**：`TorrentStatus.DOWNLOADING.value` 是中文 `下载中`，`TRANSFER` 是 `可转移`。插件同时识别中英文（与 `_normalize_query_status` 的别名集合一致，工具会用 MP 真实方法逐个核验）。传了 `hashs` 时 `hashs` 优先于 `status`（与 native 一致）。
- **状态映射**：只有 `waiting_library` 返回 `completed`；其余推进中状态（含上传、校验、交接、失败退避）返回 `downloading`，暂停/挂起/隔离失效返回 `paused`。推进中任务的进度封顶 99.9，不把 qB 100% 当“已完成”。
- **路径回填**：`DownloaderTorrent` 填充 `save_path`、`content_path`、`path`（来自账本里的 MP 侧本地根目录和种子名）。MP 的 `_resolve_actual_download_dir` 据此写入下载历史，不会退回 MP 的 `download_dir`。`content_path` 按 `local_root/<种子名>` 计算；若 qB 内容布局为 `NoSubfolder` 的多文件种子，真实内容路径不同，这一点未实现、未验证。
- **重复提交**：同 hash 已跟踪时返回 MP 的 `DOWNLOAD_TASK_ALREADY_EXISTS`（`下载任务已存在`），MP 据此不重复记下载历史；处于 `admitted/suspended/untracked/隔离失效` 的任务拒绝再次提交。
- **`downloader=None` 的限制（设计如此）**：模块调度器对 `list_torrents` 做列表合并、对 `download/stop/start/remove` 取首个非空结果。插件对未指定/其他下载器名一律返回 `None`，因此“全部下载器”的全局列表、native 自动整理都**看不到**本插件任务。MP 下载管理页/`downloading(name)` 在传入精确名称 `qB → 115` 时会用 `status=TorrentStatus.DOWNLOADING` 调用本插件（核对了 `application/download/tasks.py`）；是否在前端页面选择该名称显示，需要安装后人工确认。

## 本机环境专用的硬编码（不是通用插件）

以下值是写死在代码/默认值里、只对这台 NAS 的环境成立，换环境必须改代码，而不是只改配置：

- `/115`：OpenList 里 `115 Open` 存储的挂载路径；`validate_storage` 要求恰好一个启用的 `mount_path == "/115"` 且 `driver == "115 Open"`，`staging_path`/`target_path` 必须以 `/115/` 开头。
- `115 Open`：驱动名字符串，按 OpenList v4.2.7 的驱动名写死。
- `/115`、`/115_rclone`、`/vol02`：本机的 115 挂载/大容量卷；`local_root` 不能位于其下（`_runtime` 写死拒绝）。
- 默认路径 `/115/.qb115-staging`、`/115/媒体库监控目录`，以及凭据来源插件 `U115OfflineDownloader` / `CloudLinkMonitor`。
- raw_url 主机默认允许名单（`115.com`、`115cdn.com`、`115cdn.net`）是按 115 域名族写的，**未用真实链接核实**。

## 安全边界

- 只注册自己的 `type=qb115virtual` 条目，新条目 `default=False`；不改其他下载器、默认值、站点、订阅。名称冲突拒绝运行，不强行改类型。
- 物理下载器必须明确填写现有 qB 名称。复用已经运行的 `QbittorrentModule` 及其实例，不另收账号密码，也不经过插件 dispatcher。
- 直接对实例 `add_torrent(...is_paused=True, tag=['mp-qb115'], category=None, ignore_category_check=False)`；复用 native 选集辅助函数。**不调用 native `download()` 的重复任务分支**，因为它会给重复任务补全局 `TORRENT_TAG`。已有 hash 不自动认领；提交未确认则挂起。
- qB 专用标签固定 `mp-qb115`，不继承调用方标签或全局标签。全局 `TORRENT_TAG` 必须非空且不能包含本标签；运行时复查任务标签和实际 `save_path` 的正反向映射。
- MP 本地文件系统监控目录不能与本地根目录重叠，包括 `/` 根监控。还必须人工确认其他插件/程序没有监控或自动清理该本地目录和云端 staging；未勾选确认则拒绝准入。
- 源文件只来自 qB 文件清单，不递归扫描，不跟随任意层级 symlink。拒绝绝对路径、`..`、反斜杠、重复危险路径。仅允许代码 `MEDIA_SUFFIXES` 中的视频/音频/字幕；选中的 `.torrent`、resume、NFO、其他未知扩展名会阻塞整批，而不是上传它们。PT 种子、tracker、passkey 不交给 OpenList/115，不进任务账本。
- 暂存和监控目录必须互不包含，都在同一个 `/115` 的 `115 Open` 存储上，且下面没有嵌套存储；运行时只读 OpenList 存储登记确认这一点，不把跨存储 copy 当原子交接。两个根目录需事先人工建立。
- 只为清单已知的目录建立/枚举路径；不递归进入未知远端目录。整批交接前拒绝未知远端文件，避免将残留 `.torrent` 等一并搬入监控目录。
- `Overwrite:false`、`skip_existing:false`、`merge:false`。HTTP 成功和 JSON `code=200` 必须同时满足。已有暂存文件只在大小/哈希/可读性验证通过后复用；不自动覆盖冲突对象。目标已存在时 OpenList v4.2.7 的 PUT（`server/handles/fsup.go`）返回的是 **JSON `code=403`、message `file exists`（HTTP 200，`ErrorStrResp` 固定 `c.JSON(200, …)`）**，不是 409；本插件只把它当作“未成功”的拒绝，从不据此覆盖。
- **OpenList 地址**：令牌随每个请求发送，所以明文 `http://` 只接受回环/私有/链路本地 IP（主机名必须解析到的全部地址都是这类），其它必须 `https://`；不满足则拒绝并在页面显示固定原因（不回显地址）。注意：`ipaddress` 把 Tailscale/CGNAT 的 `100.64.0.0/10` 视为**既非私有也非全局**，所以 `http://100.x.x.x:5244` 这类原点同样被拒（不同的 `100.64.0.0/10` http 原点都会被拒），请改用 https 或内网/回环地址。
- **raw_url 只读探测的主机限制**：不带令牌的 Range 读取只会连接 OpenList 同主机，或 `115.com`/`115cdn.com`/`115cdn.net` 域名族，或配置项 `raw_url_allowed_hosts` 里额外列出的主机。OpenList 对 web 代理存储返回自身 `/p/...` 地址（`fsread.go`），对 115 返回 115 SDK 给出的 CDN 直链（`115_open/driver.go`）。真实 CDN 域名未核实：若实际域名不在名单里，任务会失败并显示“raw_url 主机不在允许名单”，把真实主机加入 `raw_url_allowed_hosts` 即可，不会放行任意主机。**“OpenList 同主机”模式把端口固定为凭据里 OpenList origin 的端口**（未写端口按 http=80/https=443）：同一台机器上的其他端口（ssh、管理面板等）不会被探测；非法端口号直接拒绝。该限制只对同主机生效，CDN 域名族和 `raw_url_allowed_hosts` 里的主机不限端口。真实 web 代理存储的 `/p/...` 地址就在 OpenList 同一端口上，不受影响。**`site_url` / `web_proxy` 的影响**（`fsread.go`）：存储开启 `web_proxy`（或驱动必须代理）时，`raw_url` 是 `<GetApiUrl>/p/...`，其中 `GetApiUrl` 在 OpenList 设置了 `site_url` 时就是 `site_url`，否则取请求的 `X-Forwarded-Host/Host`；因此 OpenList 设了 `site_url`（例如公网域名）时，`raw_url` 会指向那个域名而不是插件使用的 OpenList 地址，会被“主机不在允许名单”拒绝，**需要把该域名加入 `raw_url_allowed_hosts`**（额外名单不受同主机端口固定约束）。
- 在目录 move **之前**持久化 `commit_intent`。崩溃/重启后用 `stat(暂存)` + `stat(目标)` 三态判定，永不重新上传：
  - 目标在、暂存不在 → 已搬运（可能已被 CLM 消费），宽容校验后进入“等待入库”；
  - 目标不在、暂存在且 `verify_batch` 完整 → move 从未发生，**安全重放一次非覆盖 move**；暂存不完整则挂起；
  - 两者都不在 → 挂起（可能被 CLM 搬走，也可能丢失），等人工按 SHA1/整理历史核对；两者都在 → 挂起；
  - 核对本身遇到 OpenList 超时/5xx/传输错误（`transient`）时只退避重试，不挂起、不动作。
  move 成功后若目标已被 CLM 取走（目录或部分文件消失），进入“等待入库”而不是失败；只有“存在但内容不符”才挂起。
- **没有任何本地/115 文件删除实现**。下载器删除入口 `delete_file=True`（包括省略参数）拒绝；明确 `False` 仅暂停 qB 并停止跟踪，保留源文件、云端文件、任务墓碑和空间预留。**墓碑（`untracked`）和 `suspended` 是粘性状态**：在途 tick（`_process`/退避/交接收尾都在账本锁外运行）写入的状态迁移**不会被在途**的 worker 覆盖回 `uploading/verifying/failed`，被拒绝的迁移连同其 `error` 文本一起丢弃；只有显式的 remove/abandon（→ untracked）、挂起（→ suspended，且不会把 untracked 变回 suspended）和 `unsuspend` 能跨越。上传/哈希中途出现墓碑或挂起，worker 在下一个读块/下一个文件前退出（已发出的那一个 PUT 仍会结束）。

### native 自动整理隔离的验证与条件

`tools/check_moviepilot_contract.py` 从当前 MP 容器只读固定源码，提取真正的 `list_torrents` / qB 标签过滤方法，在本地用合成任务执行：默认 `TRANSFER` 只返回全局标签任务，不返回 `mp-qb115`；自动整理队列没有请求 `include_all_tags=True`。这不是对其他插件或手工全标签整理的拦截。

**不要在任务运行期间清空/改成专用标签作为全局 `TORRENT_TAG`，不要手工给本任务补全局标签，不要让其他程序移动/清理本地源文件。** 本插件运行时会拒绝失去隔离的任务，但不能原子阻止管理员或其他插件并发修改系统标签/目录后立即发动整理。安装后的实际 NAS 目录、映射、第三方监控关系尚未验证，不能直接宣称生产隔离已经验收。

### 已知未解决风险：同 hash 经 native `download()` 重复提交会补全局标签

读容器源码确认：如果有人（订阅、手动下载、默认 qB 下载器）把**同一个 hash** 再提交给**同一个物理 qB 名称**，native `download()` 的“已存在”分支会给该任务补全局 `TORRENT_TAG`（`modules/qbittorrent/__init__.py` 重复分支）。此后：

1. native `TRANSFER` 查询（`chain/transfer/queue.py`，不传 `downloader`，会查所有 qB 实例）会选中它，可能整理本地源文件；
2. 若整理模式是“移动”，结算阶段 `remove_torrents(hash, downloader=物理名)` 默认 `delete_file=True`，**可能删除本地源文件**（本插件的 `remove_torrents` 不拦截这条路径，因为它发给的是物理 qB 模块而不是本虚拟下载器）。

**这个风险无法被本插件完全消除**（插件无法拦截别的模块对物理 qB 的调用，也无法原子地阻止别人给任务加标签）。已做的缓解如下，作用只是缩小窗口并让状态可见：

- 独立的 `Qb115DownloaderGuard` 每 10 秒只读物理 qB（不依赖 OpenList），对账本里**所有未停跟踪的任务**（含已交接、等待入库的——它们正是 native 整理的对象）按每批 50 个 hash 查询（请求数 O(N/50)）。发现任务带全局标签或丢失专用标签时：
  1. **先直接暂停该任务**；
  2. 若带全局标签，再**只对该任务**调用 qB 实例的 `remove_torrents_tag(ids=<hash>, tag=[<全局标签>])`（核对过 `modules/qbittorrent/qbittorrent.py` 的签名，底层是 `torrents_remove_tags`），这是**可逆的摘标签，不删除任务也不删文件**，永远不会摘 `mp-qb115`，摘完会回读确认；
  3. 标记 `quarantined`（页面/API 标红“隔离失效”），记结构化事件 `event=quarantine`（含 `untag=removed/failed/none`，不含 URL/凭据），任务不再上传/交接，拒绝 `start/retry`，同 hash 再提交也被拒绝。
  `tick` 里上传前/提交前的快照遇到隔离丢失时走同一套“暂停→摘标签→隔离”流程。摘标签失败时页面直接显示“⚠ 摘除全局标签失败，请立即在 qB 人工摘除”，下一次巡检仍会重试；之后又被人加回全局标签会再次处理。
- **窗口时长与残余风险（仍不能完全消除）**：qB 的 `completed` 过滤包含暂停/停止的已完成任务（`get_completed_torrents` 的排除状态集合里没有 `pausedUP/stoppedUP`，契约探针会断言这一点；qB 端过滤行为本身未实测），所以**暂停并不能阻止 native 整理**，真正起作用的是第 2 步摘标签。从“全局标签出现”到“被摘掉”之间有一个窗口：最坏约一个巡检周期（10 秒）加上一轮 qB 查询/暂停/摘标签的耗时；巡检失败、qB 不可达、`get_runtime_setting('TORRENT_TAG')` 不可用时窗口**没有上限**，此时页面显示“隔离巡检失败”（事件码 `guard_failed`）。窗口内如果 MP 已经开始整理（尤其“移动”模式的 `delete_file=True` 结算），本插件无法撤回。
- `pause/stop` 直接对物理 qB 下发，不要求隔离快照通过，隔离丢失时依然能暂停。
- **人工步骤**：出现“隔离失效”后 ① 立即在 MP 暂停/关闭 native 整理的“移动”模式并检查整理队列；② 检查本地源文件是否仍在；③ 确认 qB 里该任务没有全局标签（插件已摘除则无需处理，失败则手工摘）；④ **确认任务仍带 `mp-qb115` 标签——如果丢了，必须先在 qB 手工补回 mp-qb115 标签**（插件不会自动补回；这是选定的方案：人工补回后再 release）；⑤ 再 `POST /control?action=release`：只有 qB 里找得到该 hash、带 `mp-qb115`、且无全局标签时才会通过，否则保持隔离；通过后任务仍然暂停，再 `action=start` 恢复。已被整理/移动的任务按 MP 整理历史人工处理。
- **`TORRENT_TAG` 配置的后果**：全局 `TORRENT_TAG` 必须非空且不含 `mp-qb115`。如果被清空，或被改成包含 `mp-qb115`，适配器会对账本里**所有**未停跟踪任务报告 `tag_config_unusable`，guard 随即暂停并隔离**每一个任务（包括保种中的）**，即**暂停所有任务包括保种**（它们要靠 `release`+`start` 逐个恢复，且 `release` 要求配置先改回）；`_runtime` 同时拒绝一切新准入。改全局标签前先 abandon/暂停账本里的任务或接受这一后果。
- **布局绑定用的是 qB 的全局偏好**：账本里的 `layout` 来自 `get_content_layout()`，即 qB 的全局“内容布局”偏好，不是每个种子自己的设置。**改动 qB 全局内容布局，下一次 tick/`unsuspend` 校验时会把所有活动任务判成布局不一致并挂起所有任务**（并请求暂停它们的 qB 任务）；改回原值后用 `unsuspend` 逐个核对恢复。
- **已停跟踪（untracked）任务的轻量 guard**：guard 也会查询 untracked 任务，但**仅当**它仍带 `mp-qb115` 标签、源文件（`local_root/<种子名>`）仍在、且再次出现全局标签时，才暂停并只摘这个全局标签（不置隔离标记、状态仍是 untracked，错误文本写明）。如果你想让 MP 原生整理接手一个 abandon 过的任务，**先在 qB 里手工摘掉 `mp-qb115` 标签**（这时 guard 认为任务已被人接管，不再干预），再给它全局标签。
- 建议整理方式使用“复制/硬链接”而非“移动”，并避免其他下载器配置与物理 qB 同名重复提交同一资源。

## 禁用、配置无效、重载被阻塞时 guard 与 poll 的语义

MoviePilot 只给 `get_state()` 为真的插件注册 `get_service()` 的定时任务（契约探针核对 `projection.py`）。所以本插件的 `get_state()` = “已启用 **或** 账本里还有任务（含 untracked）”：

| 情形 | `poll`（推进上传） | `guard`（隔离巡检，10 秒） | 下载器条目 / 路由 |
|---|---|---|---|
| 启用且配置有效 | 运行 | 运行 | 已注册，`download/list/stop/start/remove` 生效 |
| 禁用状态（用户关开关） | **不注册**，不推进上传 | 账本非空则保留；**只做保护性暂停与摘全局标签**，不推进上传、不接新任务 | 条目被移除；调用一律返回 `None` |
| 配置无效 / 配置不完整（页面显示“配置无效/配置不完整”） | 不注册 | 账本非空则保留（适配器不可用时页面显示“隔离巡检失败”，这是真实的保护缺口） | 不注册条目 |
| **重载被阻塞**（旧 worker 65 秒内未释放，如长 PUT 或多 GB SHA1；或旧 qB 提交未释放账本锁） | 不注册（旧 Engine 已 `stopped`，推进也是空操作） | **保留**，沿用旧配置和旧 Engine 继续保护 | 沿用旧状态；页面显示“旧上传仍未停止…”，上传结束后**重新保存配置**才会重载 |
| 账本为空 | 视启用状态 | **不注册**；即使注册，空账本也不创建适配器、不产生 `guard_failed` 告警 | — |

要点：旧 worker 现在在 `stopped` 后会在**下一个任务之前**、**下一个 1 MiB 读块之前**、下一个文件的 PUT 之前退出，`_recover_commit` 在重放 move 之前也会复查，因此一般几秒内释放；仍然无法打断一个已经发出、正在等 OpenList 响应的 PUT（最长受 `put_timeout_seconds` 约束）。禁用状态下 `/control` API 与 MP 的 stop/start/remove 不可用（插件已不拥有下载器名），需要直接在 qB 操作。

**禁用/关闭启用开关会移除 `qB → 115` 下载器条目，但不会删除账本，也不会暂停 qB 里已有的任务。**

## 配置

| 字段 | 说明 / 默认 |
|---|---|
| `enabled` | `False` |
| `physical_downloader` | 已启用物理 qB 的精确配置名称；无默认回退 |
| `local_root` | MP 内可读的独立本地目录；必须已存在、绝对路径、各级均非 symlink |
| `staging_path` | `/115/.qb115-staging`，不得被任何 CLM/其他程序监控 |
| `target_path` | `/115/媒体库监控目录` |
| `credential_source` | `U115OfflineDownloader` 或 `CloudLinkMonitor`；只在内存读取该插件现有 `openlist_url/openlist_token` |
| `low_water_gib` | `10`；整数 ≥ 0；每次准入还会扣除账本中所有任务的全量预留 |
| `poll_interval` | `30` 秒；整数 ≥ 10；单 worker |
| `put_timeout_seconds` | 整数 ≥ 0；`0` = 按文件大小估算（至少 600 秒，按 ≥1 MiB/s 估算，上限 24 小时）；其余请求仍用 60 秒短超时 |
| `raw_url_allowed_hosts` | 逗号分隔的额外允许主机；`host` 精确匹配，`.example.com` 含子域；默认只有 OpenList 同主机和 `115.com`/`115cdn.com`/`115cdn.net` 族 |
| `isolation_confirmed` | `False`；需人工确认本地/暂存目录不受其他自动监控和清理 |

**配置校验**：`put_timeout_seconds` 必须是 ≥0 的整数，`low_water_gib` ≥0 的整数，`poll_interval` ≥10 的整数（表单里的纯数字字符串可以），`raw_url_allowed_hosts` 必须是合法主机名。任何一项非法都**拒绝启用**（插件保持禁用、不注册下载器条目、不启动 poll），页面顶部显示“配置无效，已拒绝启用：…”和原因；不再把过小的轮询间隔静默改成 10。

**配置前置检查（启用时、注册下载器条目之前，纯静态不做 IO）**：`physical_downloader` 非空；`local_root` 是非根绝对路径且不在 `/115`、`/115_rclone`、`/vol02` 下；`isolation_confirmed` 勾选；`credential_source` 在允许集合内；`staging_path`/`target_path` 在 `/115/` 下。任何一项不满足就**不注册** `qB → 115` 条目、`_enabled=False`，页面显示“配置不完整，已拒绝启用（未注册下载器条目）：…”。不会再出现 MP 里已有条目、却每次提交失败、guard 每 10 秒记 `guard_failed` 的状态。**写 Downloaders 列表使用 MP 的 `SystemConfigOper.update_atomically(key, mutation(session, current))`**（契约探针核对签名与写锁）：在 MP 的配置写锁里基于最新值计算自己的条目，不会用过期副本覆盖别人同时做的下载器修改；无需变更时完全不写。

不会复制凭据到本插件配置、持久化任务、日志、页面或 API。OpenList URL 只接受不含账号、密码、query 的 origin；不接受子路径反代。管理接口令牌需要存储列表读取、文件列表/读取、创建目录、上传、移动权限；没有这些权限就 fail closed。

下载目录**始终使用插件独立 `local_root`**，不继承 MP 调用方普通目录或分类；实际路径由已配置物理 qB 的 `normalize_path / normalize_return_path` 双向核对。请在安装后的人工验证中确认 MP 和 qB 指向同一份本地文件，不能仅凭两个同名路径推断。

### OpenList v4.2.7 上传头

核对了版本源码 `server/handles/fsup.go`：完整 SHA1 头是 **`X-File-Sha1`，不是 `Hash-Sha1`**。本实现发送 URL encoded `File-Path`、`As-Task:false`、`Overwrite:false`、准确 `Content-Length`、完整 `X-File-Sha1`，通过 `http.client` 有界读取文件流。`115 Open` 驱动从 `file.GetHash()` 取 SHA1；缺完整哈希时会 `CacheFullAndHash`，因此不要改回错误的头名。

**为什么 PUT 需要长读超时**：v4.2.7 的 `As-Task:false` 上传是同步的，响应在文件真正传完 115 之后才返回（115_open 的 `Put` 先 `RangeRead/CacheFullAndHash` 再推 OSS）。因此 PUT 在连接成功后把 socket 读超时改为 `put_timeout_seconds` 或按文件大小估算值；连接阶段、列表/查询/移动请求仍然是短超时。如果仍然超时（无响应，结果未知）：该任务进入退避，**从失败发生的那一刻起**（不是从本轮 tick 开始起，PUT 可能阻塞数小时）至少静默 5 分钟，期间绝不对同一路径并发重传；静默期写在账本单独的 `quiet_until` 字段里，`start`/`retry` 只清除普通指数退避 `next_retry`，**不会**解除静默期。静默期过后先检查目录列表，已存在就走完整校验后复用，不存在才重新 PUT（`Overwrite:false`，对象若恰好已落地会被 OpenList 以 `code=403 file exists` 拒绝而失败退避，之后复用）。HTTP 返回 5xx 的失败（请求已结束）不加静默期。注意：不确定客户端断开后 OpenList 是否取消仍在进行的 115 上传（按源码推测会取消），未用真实 115 验证。

校验 `/api/fs/get` 和 raw URL 使用同一 User-Agent（115 链接签名依赖它）；raw 请求只读至多 4096 字节，不带 OpenList token，不跟随重定向。可用远端 SHA1 会比对；远端不提供 SHA1 时只能证明大小 + 文件头，不声称做了完整远端重读。

参考：
- https://github.com/OpenListTeam/OpenList/blob/v4.2.7/server/handles/fsup.go
- https://github.com/OpenListTeam/OpenList/blob/v4.2.7/server/handles/fsread.go
- https://github.com/OpenListTeam/OpenList/blob/v4.2.7/drivers/115_open/driver.go

## 持久化、重试和控制

任务使用插件 `get_data/save_data` 的 `tasks_v1` 键；落在 **MP 数据库的 `plugindata` 表**，一行 `(plugin_id='Qb115Downloader'（类名）, key='tasks_v1', value=<JSON>)`（契约探针核对 `PluginData` 模型和 `save_data` 的默认 plugin_id）。备份/迁移就是备份这一行。账本字段（与实现一致）：

- 身份/预留：`hash`、`name`（种子显示名）、`reserved`（全种子大小）、`root`（MP 侧本地根目录）、`selected_size`；
- 绑定：`physical_downloader`、`layout`、`save_path`——**`save_path` 是准入时由配置经 qB 路径映射推算出的“预期路径”（去掉结尾 `/`）**，每次快照都校验 qB 实际路径与它一致（两边都去掉结尾 `/` 后比较，qB 返回带斜杠不会误挂起），不是首次快照后回写的实际值；
- 状态：`state`、`paused`、`quarantined`、`error`、`attempts`、`next_retry`（普通退避到期时间）、`quiet_until`（PUT 结果未知的静默到期时间，只由失败设置，`start/retry` 不清除）；
- 上传：`manifest`（冻结的选中文件清单：source/remote/size/sha1/head/signature）、`stage`、`target`、`verified`（已完成远端校验的 remote 名，重试时若目录列表里大小一致就不再重新探测）、`commit_intent`；
- 不保存 torrent 内容、cookie、tracker、raw URL 或云端 token。

- 磁力、v2/未知大小种子拒绝；v1 `bytes` 或本地 `Path` 可接收。以**全种子大小**预留空间（即使只选几集也保守预留）。
- 空闲空间必须覆盖低水位 + 现有全部预留 + 新任务。已经上传/继续保种/停止跟踪的预留都不自动释放，因此可能明显过度保守，不会只靠当前 free 无限接单。
- 网络失败退避 `60s → 120s → ... → 最多 3600s`；完成文件逐个重新验证后复用。**不支持远端分片断点续传**；中断文件需整文件重传，已有冲突/半成品不会被自动覆盖或删除。
- 上传清单冻结。选集、源文件或文件签名变化会挂起；不偷偷替换清单。**文件签名只含 inode/大小/mtime_ns**（不含 `st_dev`、`st_ctime_ns`）：新增硬链接、chmod 或 btrfs 子卷设备号变化不会再让已上传任务误挂起。取舍：保持 inode、大小和 mtime 不变的原地改写无法通过元数据发现（逐文件 SHA1 仍在上传前计算，已上传文件的云端 SHA1/大小/文件头在交接前校验）。旧版 5 字段签名在加载时自动迁移。
- **任务与物理 qB 绑定**：账本为每个任务持久化 `physical_downloader`、qB 实际 `save_path`、`layout`，每次 tick/控制前比对；改配置/换物理下载器/保存路径或布局变化 → 任务挂起，且不对另一个实例执行 pause/start。账本缺少这些字段同样挂起。重载插件会丢弃旧的 qB 适配器。
- **重复 hash 预检在预留之前**：物理 qB 已有同 hash 时直接拒绝，不写账本、不占预留。`add_torrent` 被拒绝且确认 hash 不存在时回滚账本；添加后确认快照有短重试（5 次 × 0.4 秒）。添加状态不明（已添加但选集/快照未确认）仍保持挂起并保留预留，不自动重复添加。
- **并发**：账本的拷贝与写入统一在 `engine.lock` 下；锁内只读写账本、切换状态，以及在写入 `commit_intent` 前最后复查一次“已暂停/已隔离/已停止”。哈希、上传、**提交阶段的 qB 快照、逐文件本地签名、`verify_batch` 列目录、`stat(目标)`、`move`**都在锁外（测试证明慢 verify/stat/move 期间锁可获取，`pause` 立即返回）。边界：`commit_intent` 落盘之后到来的 `pause` 撤不回已发出的 `move`；在它之前到来的 `pause`/隔离会阻止 `move`。仍在锁内做 qB 网络调用的只有 `download()` 提交（添加/选集确认，秒级）以及 `control()` 里的 qB 暂停/启动 RPC。
- **暂停/启动不依赖 OpenList**：`/control`、MP 的 `stop/start/remove` 按需创建只含物理 qB 的适配器，OpenList 不可达或重载后未 poll 仍可 pause/start。
- **预留空间显示**：任务一旦冻结了上传清单（选中文件已完整落盘，`free` 已包含），不再重复计入新任务准入；`reserved` 字段保留原值作历史显示。未冻结清单的任务仍按全种子大小计入。该机制只影响新任务准入判断，不自动删除/释放任何东西。
- **115/OpenList 调用频率**：`stat` 先发一次 `/api/fs/get`（OpenList 的 115 Open 驱动直接问 115，不扫目录），识别到 `object not found` 就直接判不存在；只有遇到无法识别的拒绝才对父目录列一次表确认。列目录只有第一页 `refresh=true`，后页不 refresh。同一轮同一目录只列一次表并缓存在内存（不是跨轮缓存）。监控目录**从不被整目录扫描**（目标检查用 `fs/get`，交接后只列目标批次目录一次）。已验证文件在重试时不重复探测。本地 loopback 测量（24 集单目录一次完整提交）：24 次 PUT、3 次 refresh 列表（此前 79 次）、30 次 `fs/get`、24 次 raw 可读性探测、1 次 move，监控目录列表 0 次。不递归、没有大量 refresh；真实 115 的限流阈值未测。`not found` 文本识别来自 OpenList 源码（`errs.ObjectNotFound` → `FsGet` 的 500 响应），未对真实 115 验证；识别失败时会退回到“父目录列一次表”，功能不坏，只是请求更多。**OpenList 侧的代价（读 v4.2.7 源码，未实测）**：`fs/get` 先查 OpenList 的**目录缓存**（`op.Get` 命中缓存里的旧条目就直接返回，不问 115），未命中才问驱动；对**文件**，`FsGet` 为了给出 `raw_url` 还会 `fs.Link`，对 115 Open 驱动这会发起一次 **115 下载链接请求**（有链接缓存，首次必发）。所以每个文件的校验 = 一次 `fs/get` + 一次 115 下载链接请求 + 一次 raw 探测，上传大批量文件时要考虑 115 的限流。**move 的“目标已存在”预检是非原子的**：`FsMove` 在 `overwrite=false` 时先 `fs.Get(目标)` 再执行 move，两步之间别的写入者（CLM/人工）可以创建同名目标；插件自己也做一次 `stat(目标)` 预检，同样非原子，真正的兜底是 `overwrite=false` 与交接后的 `count_present` 核对。
- **guard/状态页的开销**：guard 按每批 50 个 hash 查 qB（只查未停跟踪任务）；`tasks_status`/`list_torrents` 不再逐任务 `get_files`，只用一次批量 qB 状态查询（5 秒内缓存，页面刷新共享），请求数 O(N/50)。账本里已完成的任务不会自动清理，所以长期使用 N 会增长；巡检仍按批进行，保护范围不缩小。
- OpenList 存储校验结果缓存 10 分钟（按 origin/暂存/目标路径为键），不再每 30 秒对 `/115` 与监控目录 `refresh=True` 全量列目录；暂存/目标路径或来源变化立即重新校验。
- 结构化日志 logger `qb115downloader`：只有事件码、hash、状态、暂停/隔离标记，不输出 URL、token、异常文本。
- `pause` 持久化暂停并请求暂停 qB；当前已开始的一个 PUT 可能结束，但不继续下一文件或交接。与已经提交的 move 的竞态以 commit 锁为边界，不能撤回已发出的 move。
- `start` 可以恢复 qB 下载/保种；对已交接任务不会重新上传。**`start`/`retry` 拒绝 `suspended` 和 `admitted`（含添加后选集未确认就崩溃的任务）**——否则会 resume qB 把整个种子下载下来。**挂起时插件会尽力请求暂停 qB**（仅限任务绑定的那个物理 qB 实例；任务页的说明写明“已请求暂停”或“⚠ 暂停失败”，不会碰另一个实例）。挂起任务的出路只有两条、都需要人工且不含删除：`abandon`（转 untracked，保留文件/云端/qB，预留不释放）和带显式确认的 `unsuspend`（见下）。`retry` 只解除普通网络失败退避，不能解除交接未知或清单变化导致的安全挂起，也不会解除上传结果未知的静默期。
- 停用/重载停止新准入与后续交接；同实例重载会等待旧上传 worker 和 qB 提交账本写入，超时则**本次重载不生效**（旧配置保留，guard 保留，见上表）。只支持单 MP 进程、单插件实例；不声称具备多进程分布式锁。

API 使用 MP 的 `apikey` 认证（不要把令牌放在 URL）：

- `GET /api/v1/plugin/Qb115Downloader/tasks`
- `POST /api/v1/plugin/Qb115Downloader/control?hash=<原始hash>&action=pause`
- `action=start` / `action=retry` / `action=release`（解除隔离标记，仅限实时检查通过）同上。非法动作/删除拒绝。
- `action=abandon`：**人工、不含删除**的“放弃跟踪”，把任何未停跟踪的任务转成 `untracked`：保留本地文件、云端文件/暂存、qB 任务，**预留不释放**，不再上传/交接，不能再 start；对任务绑定的 qB 实例尽力暂停（实例不符则只写账本）。同样 `remove_torrents(delete_file=False)` 也只做这件事。
- `action=unsuspend&confirm=<完整 hash>`：**最小安全版**，缺少或不等于 hash 的 `confirm=` 一律拒绝。引擎还要求全部成立才放行：状态 `suspended`、未被隔离、已有冻结清单、从未发起过交接（无 `commit_intent`）、实时 qB 快照通过隔离/绑定检查（同一物理实例、保存路径、布局）、qB 当前选中文件与冻结清单（文件名+大小）完全一致、每个本地源文件的 inode/大小/mtime 仍等于冻结签名。通过后任务回到 `downloading` 且**保持暂停**，qB 不会被恢复，仍需显式 `start`；之后第一次推进会对冻结清单重新做 SHA1 比较，不一致会再次挂起并暂停 qB。交接状态不明（已有 `commit_intent`）、选集从未确认（无清单）的挂起任务**不提供** unsuspend，只能 abandon 或人工在 qB/云端核对。

## 明确未完成 / 必须人工的部分

1. **最终入库/最终路径关联未自动实现。** OpenList 通用 `ObjResp` 不提供可稳定追踪搬移的 115 fileid；没有可靠的 cloud fileid → CLM → `transferhistory` 证据链，故 API 永远不报 `library_confirmed=True`。上传和云端登记只到“等待入库”，不是“已入库”。目标消失时不推断成功，交接不明时挂起，按 SHA1、原目标路径和整理历史人工核对。
2. 源文件删除、云端删除、暂存残留删除、自动空间回收、预留释放 UI **都未实现**（`abandon` 只是不含删除的停跟踪，预留仍保留）。不要因为空间紧张就直接清账本重加，这会丢掉幂等保护。清理需另行设计和独立授权。
3. PT 做种时间/分享率策略不由本插件管理；原 qB 持续保种。下载失败不自动回退 MP 订阅、不自动换资源搜索，避免误改现有订阅。
4. 常见 NFO/封面/未知扩展名若仍处于选中状态会阻塞整批。本版媒体/字幕白名单较保守；需用户在 qB 明确取消不需要的文件并在首次冻结清单前完成选择。
5. raw URL 需要额外鉴权、重定向或 CDN 特殊规则时会拒绝校验而非带管理员 token 尝试；需要后续在授权测试环境验证兼容性。
6. **仍未做/仍有的限制**：每个文件仍有一次 `fs/get` + 一次 raw 探测（没有也不能省，这是交接前的逐文件校验）；失败重试会重新对源文件做本地 SHA1（本地 IO，不触 115）；预留空间不自动释放（`untracked`、已提交未冻结清单的任务仍计入）；账本不自动清理已完成任务；同 hash 经 native `download()` 补全局标签的风险只能缩小窗口，见上文；`download()` 在持有 `engine.lock` 时做 qB 添加/选集确认（秒级，qB 异常慢时会短暂阻塞 pause/状态查询）。
7. **未验证的契约**：全部是源码核对 + 本地 loopback/假 qB 测试。未验证：真实 115/OpenList 在客户端断开后的上传行为、`content_path` 在 `NoSubfolder` 布局下、MP 前端是否按下载器名展示本任务、`get_runtime_setting('TORRENT_TAG')` 在运行时被改动的时序、一次真实 MP 重复提交到同物理 qB 的竞态窗口大小。
8. 尚未在真实下载任务、真实 115、已安装 MP 插件、CLM/CSC/Emby 上执行端到端实验；也未验证大文件上传耗时、115 限流、断电时底层数据库的持久化保证。不得将本地 HTTP 测试冒充生产验收。

## 本地开发验证

从仓库根目录执行；仅 Python 标准库，无 pytest/pip/apt：

```sh
python3 -m unittest discover -s plugins.v3/qb115downloader/tests -v
python3 plugins.v3/qb115downloader/tools/check_moviepilot_contract.py
python3 tests/test_qb115_market_contract.py
python3 -m compileall -q plugins.v3/qb115downloader
```

第一条是真实 loopback HTTP + fake qB + MP stub 测试；第二条只读 `moviepilot-v3` 的固定源码，在宿主执行提取的纯方法，不导入 MP、不读配置凭据、不调用 qB RPC、不写容器。TDD 红绿记录见 `DEVLOG.md`。

## 安装评估与回滚（本开发步骤没有执行）

- 父代理后续独立复核并添加新版 market entry。可评估 MP 官方 `PLUGIN_LOCAL_REPO_PATHS` 本地插件仓机制；不要复制代码覆盖容器 `/app`，无需 push main 才能评估。
- **`system_version` 的依赖**：market 条目仍是 `>=3.0.0`。插件导入的是新容器布局的模块（`app.sdk.plugin`、`app.runtime.settings`、`app.application.directory`、`app.modules.qbittorrent`、`app.db.oper.systemconfig`、`app.schemas.types`），**只对照过容器 v3.2.3 的源码**；没有证据表明更早的 3.x 具备/不具备这些模块，所以没有擅自收紧版本号，低于 v3.2.3 上能否加载**未验证**（加载失败表现为插件导入报错，不会半启用）。

### 试用步骤（必须按序，真实系统上尚未执行过）

1. **先保持禁用**，备份待安装实例的插件配置与账本（`plugindata` 里 `Qb115Downloader/tasks_v1` 一行）。
2. 在 MP 确认全局 `TORRENT_TAG` **非空且不含 `mp-qb115`**（否则所有任务会被隔离，见上）。
3. 确认**整理方式用“复制”或“硬链接”，不要用“移动”**（native 整理对移动模式会 `delete_file=True` 结算，见已知未解决风险）。
4. 人工确认物理 qB 名称、路径映射、独立 `local_root`、staging 与监控目录的互相排除关系，两个云端根目录事先建好。
5. 填完整配置、勾选 `isolation_confirmed` 后再启用；页面若显示“配置不完整/无效”先按提示改，**不会**注册下载器条目。
6. **只用一个小型无私密测试种子**（v1、非 PT、非私有、已获授权）验证：提交 → 上传 → 校验 → 交接 → 等待入库；其余任务不要走 `qB → 115`。
7. **上传进行中不要保存插件配置**：保存会触发重载，旧 worker 要等当前 PUT/哈希结束；超过 65 秒则重载不生效（旧配置保留、guard 保留，页面有说明），之后需再保存一次。
8. 试用期间观察：页面“隔离巡检失败”、日志事件码 `guard_failed/quarantine`、`raw_url 主机不在允许名单`（见 `site_url`/`web_proxy` 一节）。

### 回滚与卸载

- 回滚先在插件配置中禁用，确认无在途上传、自己的虚拟下载器条目已移除，再卸载/恢复旧代码。账本、qB 原文件和云端目录均保留；禁用不自动暂停已有 qB 做种，guard 在账本非空期间仍会保护。任何本地/115 清理都另行人工确认，不能把回滚当删除操作。
- **直接卸载而没有先禁用**：MP 的卸载流程只停止插件服务，**不会**回写 Downloaders 列表，`type=qb115virtual` 的 `qB → 115` 条目会遗留，需要在 MP 的下载器设置里**手工删除**；账本（`plugindata` 行）也不会被插件自己清理。
