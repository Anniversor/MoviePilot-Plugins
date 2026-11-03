# Qb115Downloader 开发/验收记录

范围：仅本地 `plugins.v3/qb115downloader/`。未部署、未重启、未 commit/push、未修改现有插件。开发时未读取用户私密 env 或服务配置/凭据；容器仅用于读取 `/app/app` 固定源码。网络上传全部在本机临时 HTTP server，内容为测试 fixture。

## 命令定义（工作目录均为仓库根目录）

```text
U = python3 -m unittest discover -s plugins.v3/qb115downloader/tests -v
C = python3 -m unittest discover -s plugins.v3/qb115downloader/tests -p test_core.py -v
P = python3 -m unittest discover -s plugins.v3/qb115downloader/tests -p test_plugin.py -v
H = python3 -m unittest discover -s plugins.v3/qb115downloader/tests -p test_cloud.py -v
L = python3 plugins.v3/qb115downloader/tools/check_moviepilot_contract.py
```

以下是本轮工具实际运行的红绿结果；同一行先写测试、运行 RED、再实现、运行 GREEN。初始空模块导入探测曾报 `ModuleNotFoundError`，不是有效的行为 RED；补空模块和能力断言后重新获得断言失败再开始实现。没有把导入错误当成已完成测试。

## TDD 纵向增量

| 增量 / 行为 | RED 实际结果 | GREEN 实际结果 |
|---|---|---|
| 选中文件完成、priority=0 跳过 | U：1 test，1 failure，缺 `selected_files` | U：1 OK |
| dirfd 无 symlink/越界/非媒体读取 | U：2 tests，1 failure，缺 `open_source` | U：2 OK |
| 真实流式 PUT、正确 SHA1/编码/长度头 | U：3 tests，1 failure，缺 cloud 实现 | U：3 OK |
| 持久化大小预留/未知大小/超量准入阻塞 | U：4 tests，1 failure，缺 Engine | U：4 OK |
| 两个真实 fixture 文件上传、校验、一次目录交接 | U：5 tests，1 failure，缺 tick | U：5 OK |
| 第二文件 HTTP 503 不 commit、退避、只重试未验证文件 | U：6 tests，1 failure（异常未落账） | U：6 OK |
| move 后模拟进程死亡，CLM 已搬走，重启不重传 | U：7 tests，1 failure（PUT 从 2 变 4） | U：7 OK |
| 首个 PUT 中暂停，不能传下一文件/交接，可启动恢复 | U：8 tests，1 failure，缺 control | U：8 OK |
| 默认删除拒绝，明确不删只停跟踪且保留预留 | U：9 tests，1 failure，缺 remove | U：9 OK |
| 明确物理 qB、专用标签、不走 dispatcher | U：10 tests，1 failure，缺 adapter | U：10 OK |
| 实际 qB save_path 双向映射、标签归属检查 | U：11 tests，1 failure，缺 snapshot | U：11 OK |
| 绕开 native download 的重复任务全局标签分支，实例暂停添加 | U：12 tests，1 failure（仍调用 native download） | U：12 OK；旧委托测试同步重构为等价实例级断言 |
| v1 metadata 有界解析、真实 infohash/总大小、不保留 passkey | U：13 tests，1 failure，缺 metadata | U：13 OK |
| staging/monitor 必须分离、同 /115、安全路径 | U：14 tests，3 subtest failures（可把半成品传进监控树） | U：14 OK |
| 插件默认 disabled、精确 downloader 过滤、transfer 空 | U：15 tests，1 failure，缺插件类 | U：15 OK |
| 插件 download→ledger→poll→真实 HTTP 交接，不伪造 qB 进度 | U：16 tests，1 failure（插件还未委托） | U：16 OK |
| 已运行 MP qB 绑定、凭据只内存、目录监控隔离 | U：17 tests，1 failure，缺 runtime | U：17 OK；中间修复测试包动态导入作用域后复跑 |
| 虚拟下载器注册/冲突不改类型/不改默认/禁用只移除自身 | U：18 tests，1 failure，缺注册入口 | U：18 OK |
| 配置表单/状态页/API/interval 单实例服务/停止钩子 | U：19 tests，1 failure，缺 get_form 等 | U：19 OK |
| 停用发生在首个 PUT 中、跨 Engine worker 排他 | U：20 tests，1 failure（仍传两文件） | U：20 OK |
| 上传中选集变更必须挂起，不交接 | U：21 tests，1 failure（错误地 move 了） | U：21 OK |
| 添加/选集确认前崩溃的 admitted 不能重启自动认领 | U：22 tests，1 failure（错误上传） | U：22 OK |
| qB 校验/移动/错误即使文件 progress=1 也未就绪 | U：23 tests，1 failure（错误上传） | U：23 OK |
| 回归覆盖：409 / HTTP200-JSON500 / 已有错误暂存 / Engine traversal+symlink | U：27 tests 全通过；这些是前面已有边界的额外覆盖，不新增生产逻辑 | U：27 OK |
| 暂存目录未知 `.torrent` 不能随整个目录搬入监控 | U：28 tests，1 failure（错误 move） | U：28 OK |
| 上传后本地文件改变必须挂起 | U：29 tests，1 failure（错误 move） | U：29 OK |
| 重试不能替换首次冻结的选中文件清单 | U：30 tests，1 failure（错误 waiting_library） | U：30 OK |
| 交接必须同一 115 Open storage、拒绝嵌套挂载 | U：31 tests，1 failure，缺 validate_storage | U：31 OK |
| runtime 必须真的调用 storage 校验 | P：5 tests，1 failure（validator 未调用） | U：31 OK |
| 本地根 `/` 文件系统监控不能漏过 | P：5 tests，1 failure（未抛安全拒绝） | U：31 OK |
| 重载需等待旧 worker 写账本完成 | P：6 tests，1 failure（重载提前返回） | U：32 OK |
| 负低水位不能绕过空间门控 | C：22 tests，1 failure（未拒绝负值） | U：33 OK |
| poll 不能越过 qB 添加/选集提交锁 | C：23 tests，1 failure（过早进入 qB snapshot） | U：34 OK |
| 交接后 start 只恢复保种，不允许 retry 重传 | C：24 tests，1 failure（start 被错误拒绝） | U：35 OK |
| 重载还需等待在途 qB 提交的 ledger flush | P：7 tests，1 failure（重载提前返回） | U：36 OK |
| 115 raw 校验 UA 必须与取链接一致、不带 OpenList Authorization | H：3 tests，1 failure（raw 没有 UA） | U：37 OK |
| 重试不能静默更换已冻结 staging/target 根目录 | C：25 tests，1 failure（错误 waiting_library） | U：38 OK |
| 预检期间停用/重载不能继续向 qB 提交 | P：8 tests，1 failure（仍返回新增 hash） | U：39 OK |

随后只对现有测试加强：流式对象拒绝无界 read、捕获 stdout/stderr 验证响应里的假 token/passkey 不输出；这些没有新增生产行为。对 import 顺序和文件签名函数进行保绿重构。

## 最终本地结果

最终联合执行 `U && L && python3 -m compileall -q plugins.v3/qb115downloader` 返回 exit code 0；此前另执行了 `git status --short --untracked-files=all && git diff --check`：

- U：`Ran 39 tests in 13.924s`、`OK`。
- L：三条 PASS（真实 native 默认标签过滤、自动整理不使用 all-tags、SDK 抽象钩子齐全）。
- compileall 无错误。
- 工作树仅新增本插件目录；没有暂存/提交/发布。仓库根 `.gitignore` 原本忽略所有 `__init__.py`，已在本插件目录内加入局部否定规则，确保入口文件会被父代理打包/加入版本管理；未改根忽略文件。
- `git diff --check` 对已有 tracked diff 无错误；新文件尚未 add，不能把它当作对全部 untracked 内容的 git diff 审查。

## 已读真实源码 / 协议发现

- 当前容器 native 类确为 `app.modules.qbittorrent.QbittorrentModule`；运行实例从 `ModuleManager.get_running_modules` 取，不重新初始化物理服务。
- 实例 `add_torrent`、`get_torrents`、`get_files`、`start_torrents`、`stop_torrents` 和 native `_select_download_files` 签名已读源码确认。
- native `download()` 的重复分支确实会补 `TORRENT_TAG`，所以从前置 hash 检查改为实例级暂停添加，并在恢复前校验归属/路径。不是继续调用该危险分支后再删标签。
- native 默认列表按 `TORRENT_TAG` 查询；自动整理队列 `list_torrents(status=TRANSFER)` 不传 `include_all_tags=True`。L 用真实提取方法执行这一契约。
- OpenList **v4.2.7** `server/handles/fsup.go` 接收完整 SHA1 的头是 `X-File-Sha1`，不是 `Hash-Sha1`；取 raw 链接与读取需相同 UA。版本源码/地址在 README。
- OpenList `ObjResp` 没有稳定云端 fileid；不虚构 fileid/最终路径/transferhistory 关联，不宣称入库成功。本版保持等待入库/人工核对，无清理代码。

## 验证层级与未验证项

- U：标准库测试，真实 loopback HTTP 和文件流，fake qB adapter，MP SDK/schema/配置存储 stub。
- L：只读容器固定 Python 源码；在宿主运行真实方法 AST + 合成任务，确认 native 默认自动整理标签隔离及抽象钩子实现。不等于安装导入、不等于调用了真实 qB。
- 未运行生产上传、下载、目录移动、CLM/CSC/Emby、安装 UI、真实 MP 插件装载，也未读取生产路径映射/凭据来“证明”本机已经符合隔离前提。
- 本版不做自动最终入库确认、删除、云端分片断点续传、空间预留自动释放。详见 README 的限制和阻塞；不能用“上传完成”代替“入库完成”。


---

# 独立审查（passed=false）后的修复轮（A–J）

约束不变：不部署、不 commit/push、不安装、不读凭据，容器只读 `/app/app` 源码。新增测试文件 `tests/test_review_fixes.py`（48 个），并改动旧夹具（`FakeQb` 增加 `name/expected_save_path/preflight/check_isolation/save_path/layout`，`admit` 传绑定字段，`Server` 增加 `put_delay/list_delay`、容忍客户端提前断开）。

命令：`U = python3 -m unittest discover -s plugins.v3/qb115downloader/tests -v`，`R = ... -p test_review_fixes.py`。

## RED 记录（修复前实际运行）

1. 先写 44 个测试，对旧代码运行 R：`Ran 44 tests`，`FAILED (failures=4, errors=38)`。其中 errors 多为新 API 缺失（`put_timeout`、`admit(physical=…)`、`check_isolation`…），属于弱红。
2. 只给 `Engine.admit` 补上绑定字段参数（纯加字段，不改行为）后再运行 R：`Ran 44 tests`，`FAILED (failures=29, errors=12)`。这一轮才是行为级红，关键失败信息：

| 项 | 测试 | RED 的行为级失败 |
|---|---|---|
| A | `A_PutTimeout.*`、`A_EngineAfterPutTimeout` | OpenList 无 `put_timeout`/`put_timeout_for`，PUT 与其它请求同为 60 秒短超时（loopback 延迟响应复现）|
| B | `B_AdmitOrdering.test_existing_hash…` | 物理 qB 已有同 hash，账本里仍留下 `admitted/suspended` 任务和预留（`tasks != {}`）|
| B | `test_add_rejected_and_hash_absent_is_marked_not_added`、`test_add_confirmed_absent_rolls_back…` | 添加被拒、hash 确认不存在时无法回滚 |
| C | `test_pause_works_directly_even_when_isolation_is_lost` | 隔离丢失时 `pause()` 返回 False/抛 ValueError（先 snapshot）|
| C | `test_isolation_lost_discovered_during_tick…` | tick 发现隔离丢失只退避，未暂停（`paused == []`）|
| C | guard/quarantine 系列 | 无 `guard`/`check_isolation`/`quarantined`，无巡检服务 |
| D | `D_Binding.*` | 其他物理下载器接管同 hash 继续上传；save_path/layout 变化不挂起；账本缺字段仍被信任；重载保留旧 `_qb` |
| E | `E_MoviePilotContract.*` | 中文“下载中”被当成 all，把已上传等待入库任务返回；`hashs` 被 status 过滤；无 save_path/content_path/path；已跟踪返回“任务已跟踪，未重复提交” |
| F | `F_ControlWithoutOpenList.*` | `_runtime` 抛错（OpenList 不可达）时 pause/start 失败；`self._qb is None` 时 stop/start 直接 False |
| G | `test_flush_snapshot_is_taken_under_engine_lock` | flush 在锁外拷贝，另一线程 `admit` 可在拷贝过程中写入（`new in engine.tasks`，原代码会触发 dictionary changed size）|
| G | `test_tick_only_mutates_tasks_and_saves_under_the_engine_lock` | `_process` 在锁外写 task（`manifest/selected_size/state/stage/…` 共约 20 个违例）|
| H | `test_hardlink_created_during_upload…`、`signature_ignores…` | 上传期间新增硬链接（ctime 变化）→ 任务 `failed`，应为 `waiting_library`；签名含 dev/ctime |
| I | `test_intent_persisted_but_not_moved_replays…` | commit_intent 已落盘、move 未发生，重启后 `moves == 0` 并挂起而不是安全重放 |
| I | `test_clm_consuming_target_right_after_move…` | move 后目标立刻被 CLM 取走 → `failed`，应为 `waiting_library` |
| I | `test_transient_probe_failure_never_suspends…` | 核对时的瞬时 OpenList 故障把任务 `suspended` |
| J | `test_reservation_not_counted_twice…`、`test_non_media_selection…`、`test_structured_log…`、`J_StorageValidationCache` | 预留永久双重计数；非媒体文件在哈希之后才被拒；无结构化日志；每次 `_runtime` 都重新校验存储 |

修复前即通过的测试（刻画既有安全行为，保持不回退）：`test_incomplete_stage_is_not_moved_on_recovery`、`test_stage_and_target_both_absent_suspends`、`test_ambiguous_failure_keeps_suspended_reservation`、`test_add_rejected_but_hash_present_is_ambiguous`。

## 绿灯与"先写实现再补测试"的诚实说明

- 修复后：R `Ran 44 tests / OK`（随后补充 4 个测试后为 48 个）；U（含旧 39 个）`OK`。
- **未走红灯就写的测试**：`C_Release`（3 个：release 动作、start 遇到隔离丢失、已隔离任务拒绝再次提交）和 `test_long_upload_io_does_not_hold_the_engine_lock` 是在对应实现之后补的回归测试，不是红→绿。`release` 是为了让隔离标记有人工出口而新增的动作。
- 契约探针 `tools/check_moviepilot_contract.py` 扩展：状态枚举改用容器 `schemas/types.py` 里的真实 enum（原来手写的 stand-in 没有中文值）；新增核对 `DOWNLOAD_TASK_ALREADY_EXISTS`、中英文状态别名经 MP 真实 `_normalize_query_status`、`downloading()` 调用形状、模块方法聚合语义（`list_torrents` ORDERED_LIST_MERGE，其余 FIRST_NON_EMPTY）、插件方法签名覆盖必需参数、`_resolve_actual_download_dir` 回读 `save_path`、`_select_download_files`/`add_torrent`/实例路径助手签名，以及“重复分支补全局标签、移动模式默认 delete_file=True”这两个已知危险仍然存在（探针断言它们仍存在，一旦上游修复会提示更新 README）。`submission.py` 使用 3.12 f-string，宿主 3.11 不能 AST 解析，改为文本切片核对。

## 各项处理与取舍

- A：PUT 在连接后把 socket 读超时改为 `put_timeout_seconds` 或按大小估算（≥600 秒、1 MiB/s、上限 24h），其余请求保持 60 秒。超时/传输错误标记 `outcome_unknown`，任务退避且至少静默 300 秒，之后先 stat/verify 再决定是否 PUT；HTTP 5xx 不加静默。
- B：`qb.preflight` 在 `engine.admit` 之前；`NotAdded`（add 被拒且 hash 确认不存在）回滚账本；添加后快照 5×0.4 秒重试。
- C：`pause()` 直接 stop，不依赖快照；`Engine.guard()` + 10 秒 `Qb115DownloaderGuard` 巡检（只读 qB）；`quarantined` 持久化、页面/API 标红、不再上传/交接、拒绝 start/retry/重新提交，`release` 需实时检查通过。风险只能缩小窗口，README 如实写明且给出人工步骤。
- D：账本持久化 `physical_downloader/save_path/layout`，tick/控制前比对；重载重置 `_qb`。
- E：见 README“与 MoviePilot 的接口契约”。
- F：`_qb_adapter()` 只含物理 qB，`/control`、stop/start/remove 不再依赖 `_runtime`。
- G：`flush/_set` 统一在 `engine.lock` 下拷贝并保存；`_process` 所有写 task 走 `_set`；上传 IO 在锁外。
- H：签名改为 `[ino, size, mtime_ns]`，加载时迁移旧 5 字段签名。
- I：`Engine._recover_commit` 三态，`_settle_after_move` 对目标消失宽容，瞬时错误只退避。
- J：做了——预留不再双重计数、非媒体预检先于哈希、结构化日志、OpenList 存储校验缓存 10 分钟、契约探针扩展。没做——`stat` 每文件列父目录的限制、mkdir 去重（README 如实列出）。


---

# 第二轮独立审查（passed=false）后的修复轮（1–10）

约束不变：不部署、不 commit/push、不安装、不读凭据；容器只读 `/app/app`；OpenList 源码从 GitHub `v4.2.7` 只读取回核对（`fsup.go`、`fsread.go`、`op/fs.go`、`115_open/driver.go`、`errs/object.go`）。新增测试文件 `tests/test_review_round2.py`（R1–R10，56 个含子用例）。

## 先红：修复前实际运行

1. 先只改测试夹具（不改行为，全量仍 `Ran 87 tests / OK`）：`FakeServer` 增加 `lists/gets/page_size/not_found_message/raw_base` 记录与分页；`Qb`/`FakeQb` 增加 `isolation_report/untag_global/brief`。
2. 写 R1–R10 后对**旧生产代码**运行：`Ran 56 tests`，`FAILED (failures=44, errors=15)`（子用例分别计数），8 个当时就是绿的（见下“先绿说明”）。行为级红（节选）：

| 项 | 测试 | 红的行为 |
|---|---|---|
| 1 | `R1.test_put_hung_for_hours…` | 可推进时钟让 PUT “阻塞 3 小时”后超时：旧 `next_retry`=1300（tick 开始 1000+300），早于失败时刻 11800；下一次 30 秒轮询再次 PUT 同一路径 |
| 1 | `R1.test_recover_commit_backoff…`、`…ordinary_exponential…` | 退避也是从 tick 开头算，早于失败时刻 |
| 2 | `R2.test_start_and_retry_do_not_skip…` | `start/retry` 把 `next_retry=0`，点开始立即重传 |
| 3 | `R3.*`（3 个） | qB 返回 `/path/` 或账本带结尾斜杠 → 任务被误 `suspended`；`admit` 把带斜杠路径原样入账 |
| 4 | `R4.*`（4 个） | 慢 `verify_batch`/`stat(target)`/`move` 期间 `engine.lock` 被占；`pause` 阻塞到 move 结束；verify 之后、`commit_intent` 之前来的 `pause` 无法阻止 move（旧代码 move 了） |
| 5 | `R5.*`（6 个） | `stat` 每次 `list(refresh=True)`+get；分页每页 refresh；24 集一次提交 **79 次 refresh 列表**，监控目录被整目录列表 24+ 次；重试 23 个已验证文件各被 raw 探测一次（48≠1） |
| 6 | `R6.*`（8 个） | 无分批/`brief`/`isolation_report`；`tasks_status` 逐任务 `snapshot`（含 get_files）；guard 异常 `pass`，页面无提示；untracked 仍被查询 |
| 7 | `R7.*`（8 个） | guard 只暂停不摘标签；无 `untag_global`；release 对“hash 不在 qB/缺 mp-qb115”放行（先修正测试夹具，使之能真正在旧代码上变红） |
| 8 | `R8.*`（~16 个） | 非法 `put_timeout/low_water/poll_interval` 仍启用（`low_water` 直接抛异常炸 init）；明文 http 公网地址被接受；raw_url 任意主机被连接；失败原因不可见 |
| 9 | `R9.*` | `start` 对 `suspended/admitted` 直接 resume qB |
| 10 | `R10.*`（4 个） | README 缺 403 `file exists`、环境硬编码声明、窗口说法错误（“窗口最长约 10 秒”）、账本字段 |

### 先绿说明（诚实记录）

修复前就通过的（刻画既有行为/防回退，不是红→绿）：`R2.test_retry_still_clears_an_ordinary_network_backoff`、`R3.test_adapter_snapshot_accepts_trailing_slash_from_qb`（adapter 本来就 `rstrip('/')`，问题在 Engine）、`R5.test_stat_of_a_missing_object_with_unrecognised_error…`（回退路径）、`R7.test_absent_hash…`、`R7.test_plugin_has_no_deletion_code_path`（删除类 API 的静态禁用清单）、`R8.test_boundary_values…`、`R9.test_pause_of_suspended…`。另有一个我自己的测试 bug：`R7.test_release_requires…` 一度在旧代码上也是绿的（旧夹具里 `lost` 没被清掉），改成先 `lost.clear()` 后才真正变红。

## 实现（绿）与取舍

- 1：`tick` 用 `at()`=`base + (clock()-started)` 在失败发生时（以及每个任务到期判断时）重新取时间；`Engine(clock=)` 默认 `lambda: time.time()`（测试可推进）。`_recover_commit` 也收 `at`。静默期是独立账本字段 `quiet_until`；`next_retry = failed_at + max(退避, 静默)`。
- 2：`start/retry` 只置 `next_retry=0`，不碰 `quiet_until`。
- 3：`norm_path` 两边都去结尾 `/`；`admit` 入账即规整。账本里存的是“预期路径”（配置经 qB 映射推算），不是首次快照回写——因为 adapter 每次快照已强制“实际==预期（去斜杠）”，回写值恒等，没有额外信息；README 已按此写。
- 4：提交阶段拆成 无锁校验（快照/清单/本地签名/`verify_batch`/`stat(target)`）→ 短锁内复查 paused/quarantined/stopped 并落 `commit_intent` → 无锁 `move` → 无锁 settle。`pause` 在 intent 之前到会阻止 move；之后到撤不回（README）。`download()`/`control()` 里的 qB RPC 仍在锁内（README 未做项）。
- 5：`stat` 先 `fs/get`，只对识别出的 `object not found`（源码核对）判不存在，其它拒绝才列父目录一次；列目录只有第 1 页 `refresh`；同一轮同目录一次列表缓存；`verified` 账本字段让重试不再重复探测；交接后只列目标批次目录（`count_present`）；监控目录从不整目录扫描。实测 24 集：refresh 列表 79→3，`fs/get` 30，raw 探测 24。
- 6：`QbAdapter.isolation_report/brief` 按 50 一批；guard 只扫未 untracked 任务；`tasks_status` 用一次批量 `brief`（5 秒缓存）；guard 失败写 `guard_failed` 固定事件 + `engine.guard_failed` + 页面“隔离巡检失败”。**与审查的一处偏离**：审查建议排除“完成且无工作”的任务，我没有排除 `waiting_library`——它们正是 native 整理的对象，排除会让最危险的一类失去保护；开销改由分批控制。
- 7：guard 先 `pause` 再 `untag_global`（核对真实签名 `Qbittorrent.remove_torrents_tag(ids, tag)` → `torrents_remove_tags`，只摘全局标签、回读确认、绝不摘 `mp-qb115`、不删除），结果写 `untag=removed/failed/none` 事件和任务 `error`；失败会在下次巡检重试；已隔离任务只有“全局标签再次出现”才重复动作。`release` 改用 `isolation_report`：hash 不在 qB、缺 `mp-qb115`、带全局标签都拒绝。选择了“README 人工补回 mp-qb115 后再 release”，没有自动补标签。契约探针新增：`remove_torrents_tag` 签名与 `get_completed_torrents` 不排除 `pausedUP/stoppedUP` 的断言。
- 8：`_validate_config`（≥0/≥0/≥10 整数、主机名格式）非法则 `_enabled=False`、不注册条目、页面显示“配置无效…”；`OpenList` 明文 http 限回环/私有/链路本地（主机名需全部解析到私有地址；可注入 resolver 测试）；`raw_host_allowed`：同主机 / `115.com`、`115cdn.com`、`115cdn.net` 族 / `raw_url_allowed_hosts`；被拒时 `raw_host_refused` 固定错误码，任务页面显示“raw_url 主机不在允许名单”，不回显主机。真实 raw_url 形态按源码：web 代理存储返回 OpenList 自身 `/p/…`，115 返回 SDK 的 CDN 直链；CDN 域名未实测，默认名单是 115 域名族**推测**，README 明示，名单不对时 fail closed 且有补救配置。`_runtime` 的配置类拒绝改为带固定中文原因的 `ConfigError`，`poll` 把它显示到页面。
- 9：`control` 的 `start/retry` 拒绝 `untracked/suspended/admitted`。
- 10：README：403 `file exists`（`fsup.go`；`ErrorStrResp` 固定 HTTP 200）、环境硬编码专节、账本字段、隔离窗口与人工步骤、调用频率实测、未做项。

## 为适配新行为而改动的旧测试（语义不变）

- `test_core`：`next_retry == 160` → `assertAlmostEqual(..., 160, delta=5)`（退避现在从失败时刻算，含 tick 实际耗时）。
- `test_review_fixes.A_PutTimeout…short`：短超时用例由 `api.stat` 改为 `api.list_dir`（`stat` 不再列目录）。
- `test_review_fixes.C_Release…`：guard 现在会摘标签，用 `untag_result="failed"` 让全局标签保留以保持原断言。
- 夹具：`Server`（计数/分页/not-found 文案）、`Qb`/`FakeQb`（新适配器方法）。

## 最终本地结果

- U：`Ran 143 tests`，`OK`（87 旧 + 56 新）。
- L：10 条 PASS（含新增 `remove_torrents_tag` / completed 过滤断言）。
- `python3 tests/test_qb115_market_contract.py`：1 test OK。
- `python3 -m compileall -q plugins.v3/qb115downloader`：exit 0；`git diff --check`：exit 0。
- 仍然没有任何真实 115/qB/MP 端到端验证。


---

# 第三轮独立审查（通过，可进安装试用评估）后的试用前修复（1–8）

约束不变：不部署/不安装、不 commit/push、不读凭据；容器只读 `/app/app`（`docker exec moviepilot-v3 python3` 读源码）；OpenList 源码只读取回 GitHub `v4.2.7`（`fsread.go`、`op/fs.go`、`fsmanage.go`、`common/base.go`）。新增测试 `tests/test_review_round3.py`（R3_1…R3_8，59 个）。没有引入任何删除类调用；真实系统均未验证。

## 先红：修复前实际运行

写完测试、**生产代码未改**时：`python3 -m unittest discover -s plugins.v3/qb115downloader/tests -p test_review_round3.py` → `Ran 58 tests`，`FAILED (failures=46, errors=12)`（子用例分别计数）。随后补了 1 个 CGNAT 特征化用例（修复前就绿）。

| 项 | 红的行为（节选） |
|---|---|
| 1 | 假锁（`worker.acquire` 恒 False）重载后 `get_service()` 从 `[Poll, Guard]` 变 `[]`、`get_state()` False、`guard()` 直接 return；禁用/配置无效同样 `[]`；禁用时 guard 不做保护性暂停 |
| 2 | 第一个任务在 snapshot 里置 stopped，第二个任务仍被 snapshot（`snapshot_calls` 两项）；4 MiB 文件哈希在 stopped/暂停后仍读到 EOF（8 次读，期望 ≤3）；`_recover_commit` 在 verify_batch 之后 stopped 仍重放 move（`server.moves` 1 条） |
| 3 | PUT 期间 `engine.remove(delete_file=False)` → 最终 `state=verifying`（墓碑丢失）；PUT 期间 `control(pause)` 走绑定不符挂起 → 最终仍 `verifying`；`_suspend` 能把 untracked 变回 suspended；`_set(state=...)`/`_backoff` 能把 untracked/suspended 改走并改写 error |
| 4 | `_suspend` 不请求暂停 qB（layout 漂移与 admitted 两条路径 `qb.paused==[]`）；无 `abandon`/`unsuspend`（AttributeError）；API 无这两个动作 |
| 5 | guard 不查 untracked：重新出现全局标签的 untracked 任务既不暂停也不摘标签；账本只剩 untracked 时 guard 服务被注销 |
| 6 | `enabled=True` 且无 physical_downloader/local_root/isolation_confirmed 仍注册条目；写列表用 `store.set`（读改写，假 store 的 `set` 会 AssertionError，并发写入的条目会丢）；真实 `_entry_store` 不走 `update_atomically`；账本为空时 guard 仍 `guard_failed=True` 并在页面告警 |
| 7 | 同主机不同端口的 raw_url 被放行；非法端口号走通用异常；`raw_host_allowed` 无端口参数 |
| 8 | README 缺：禁用/无效/重载被阻塞语义、目录缓存与 115 下载链接请求、move 预检非原子、墓碑粘性、TORRENT_TAG/布局后果、CGNAT/site_url/web_proxy、账本位置与卸载遗留、system_version、abandon/unsuspend、试用步骤、update_atomically（11 个 README 用例） |

### 先绿说明（诚实记录）

修复前就绿的 8 个（刻画既有行为/防回退，不是红→绿）：`R3_1.test_empty_ledger_has_no_services_and_state_false`、`R3_1.test_enabled_plugin_still_registers_poll_and_guard`、`R3_4.test_suspend_through_a_different_qb_instance_never_pauses_it`、`R3_4Api.test_no_delete_action_exists`、`R3_5.test_untracked_without_our_tag_is_left_alone`、`R3_5.test_untracked_whose_source_is_gone_or_hash_absent_is_left_alone`、`R3_6.test_name_collision_is_still_refused_inside_the_atomic_update`、`R3_7.test_same_host_same_port_still_verifies`；另 `R3_7.test_cgnat_http_origin_is_refused_as_documented` 是后补的 README 声明佐证（`ipaddress` 对 100.64/10 的 `is_private` 为 False，实测）。测试夹具自己的两处 bug（已在红之后修正，未改生产）：`R3_5` 的 OTHER 任务需要真实存在的源目录；`absent` 与 `global_tag` 不能同时出现（真实适配器对不在 qB 的 hash 只报 `absent`）。

## 实现（绿）与取舍

- **1**：`get_state()` = 已启用 **或** 账本非空（含 untracked）——已读 `runtime/extensions/plugin/projection.py`：MP 只给 `get_state()` 为真的插件注册 `get_service()`；`get_service()` 禁用时只返回 Guard；`guard()` 不再依赖 `_enabled`，禁用状态只做保护性暂停/摘全局标签（`poll` 仍要 `_enabled`）；重载被阻塞时保留旧配置、旧 Engine 与 Guard，Poll 不注册（旧 Engine 已 stopped，推进本就是空操作），页面注明“重新保存配置以重试”。`_owns()` 仍只认 `_enabled`，禁用时不接管任何下载器调用。代价：禁用时 `/control` 与 MP 的 stop/start/remove 不可用（README 如实写明）。
- **2**：`tick` 每个任务前查 `stopped` 并 `break`；`_held()` 现在包含 stopped/pause/quarantine/suspended/untracked，哈希在每个文件前和每个 1 MiB 块后检查，PUT 前沿用；`_recover_commit` 开头、`verify_batch` 之后、置 `commit_pending` 与重放 move 之前复查。
- **3**：`_set` 对 `untracked/suspended` 设为粘性：未持有权限的 `state` 写入被丢弃并连带丢弃 `error`；权限为 `_force=untrack|suspend|unsuspend`；`_suspend` 不会把 untracked 变回 suspended。选择“丢弃迁移”而非“在途 `_process` 写入前锁内复查”：覆盖所有 `_set` 调用点（含 `_backoff`、`_settle_after_move`），不依赖每处自查。
- **4**：`_suspend(task, msg, qb)`：仅当 `qb.name == 任务绑定的物理下载器` 才请求暂停，结果写进说明（“已请求暂停 qB”/“⚠ 暂停 qB 失败”）；`tick/_process/_recover_commit/download()` 都传 qb，`control` 的绑定不符路径不传（绝不碰另一实例）。新增 `Engine.abandon`（任何未停跟踪任务 → untracked，保留文件/云端/qB，预留不释放，不含删除）和 `Engine.unsuspend(confirm=True)`（要求：suspended、未隔离、有冻结清单、无 `commit_intent`、实时快照通过绑定/隔离检查、选中文件与清单（名+大小）一致、本地签名未变；成功回 `downloading` 且保持暂停，不恢复 qB）。API：`abandon`；`unsuspend` 必须 `confirm=<完整 hash>`。**没有**对“交接状态不明/选集未确认”的挂起提供 unsuspend。
- **5**：选择“轻量 guard”：对 untracked 且仍带 `mp-qb115`、源文件（`root/name`，`os.path.lexists`）仍在的任务，出现全局标签时暂停并只摘这个全局标签（不置隔离标记、不改状态）；缺 `mp-qb115`（人工接管）或 hash 不在 qB 或源文件不在则不动。README 写明“要交给 MP 原生整理，先摘 mp-qb115”。
- **6**：`_readiness()`（纯静态）：physical_downloader、local_root（绝对/非根/不在 /115 /115_rclone /vol02）、isolation_confirmed、credential_source、staging/target 在 `/115/` 下；启用且不满足 → `_enabled=False`、不注册条目、页面“配置不完整…”。Downloaders 列表改用 `SystemConfigOper.update_atomically(key, mutation(session, current))`（容器源码核对：`db/oper/systemconfig.py:157`，写锁 + `with_for_update`，mutation 签名 `(Session, current)->(result, value)`），`_AtomicEntries` 适配成 `mutation(current)`；先只读比对，无需变更则完全不写；写后回读核对。`guard()` 先看账本，空账本不建适配器、不告警。
- **7**：`raw_host_allowed(..., port, origin_port)`：同主机必须等于 OpenList origin 端口（缺省按 80/443）；CDN 域名族与 `raw_url_allowed_hosts` 不限端口；非法端口 → 固定错误 `remote readable URL unavailable`，不连接。
- **8**：README 新增“禁用/配置无效/重载被阻塞语义”表、试用步骤、卸载遗留；补 fs/get 目录缓存 + 115 下载链接、move 预检非原子、site_url/web_proxy（`fsread.go` 的 `GetApiUrl`：`site_url` 否则 `X-Forwarded-Host/Host`）、CGNAT、TORRENT_TAG/布局后果、账本位置、system_version 说明。**`system_version` 未收紧**：只有容器 v3.2.3 的源码证据，没有任何证据说明更早版本缺这些模块，README 如实注明；market 契约测试仍断言 `>=3.0.0`。契约探针新增 3 组 PASS：`update_atomically` 签名/写锁/`Downloaders` 键、账本表/键、`get_state` 与服务注册的关系。

## 为适配新语义而改动的旧测试（语义不变，仅夹具/配置）

- `test_plugin.test_registration_never_retypes_collision_or_changes_defaults`、`test_review_round2.R8_ConfigValidation` 的 Store 夹具：`set` → `update_atomically(key, mutation)`；启用用例补完整配置（旧用例用 `enabled=True` + 空配置注册条目，正是被修掉的行为）。
- `test_review_round2.R8_OriginAndRawHost.test_extra_allowed_host_is_configurable_and_validated`：补完整配置。
- `test_review_round2.R6.test_untracked_tasks_are_not_queried_for_status_or_guard`：仅加注释（该用例的 untracked 任务本地没有源目录，因此仍不被查询；有源目录的 untracked 现在会被 guard 查询，见 R3_5）。

## 最终本地结果

- U：`Ran 202 tests`，`OK`（143 旧 + 59 新）。
- L：13 条 PASS（新增 3 条）。
- `python3 tests/test_qb115_market_contract.py`：1 test OK。
- `python3 -m compileall -q plugins.v3/qb115downloader`：exit 0；`git diff --check`：exit 0。
- 删除类调用 grep（`unlink|rmtree|os.remove|torrents_delete|fs/remove|fs/rename|shutil.move|delete_torrents`，范围 `plugins.v3/qb115downloader` 的 py/md/json，不含 DEVLOG）：生产代码 0 命中；仅有的命中在**旧测试**：`test_review_round2.py` 里一个夹具对自己的临时目录 `name.unlink()`（R5 setUp，第二轮遗留）、`NativeTags.delete_torrents/torrents_delete`（断言“插件绝不调用”的陷阱）和禁用词清单本身。
- 仍然没有任何真实 115/qB/MP 端到端验证。
