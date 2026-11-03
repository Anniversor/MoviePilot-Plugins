#!/usr/bin/env python3
"""Read-only live source contract probe. No MP imports/config reads/RPC calls.

Fetch fixed /app/app source files through docker exec, then execute only the
actual list/filter method AST locally against synthetic torrent dictionaries.
This is NOT an installed-plugin or real-qB end-to-end test.
"""
import ast
import copy
from enum import Enum
import json
from pathlib import Path
import subprocess
import textwrap
from types import SimpleNamespace

ROOT = "/app/app/"
PATHS = ["modules/qbittorrent/__init__.py", "modules/qbittorrent/qbittorrent.py",
         "modules/_base/downloader.py", "chain/transfer/queue.py", "sdk/plugin/base.py",
         "schemas/download.py", "schemas/types.py", "application/download/tasks.py",
         "runtime/extensions/module/contracts.py", "chain/download/submission.py",
         "chain/transfer/settlement.py", "modules/__init__.py",
         "db/oper/systemconfig.py", "db/oper/plugindata.py", "db/models/plugindata.py",
         "runtime/extensions/plugin/projection.py", "db/base.py"]
code = "from pathlib import Path; import json; print(json.dumps({p:Path('/app/app/'+p).read_text() for p in " + repr(PATHS) + "}))"
result = subprocess.run(["docker", "exec", "moviepilot-v3", "python3", "-c", code],
                        check=True, capture_output=True, text=True, timeout=30)
sources = json.loads(result.stdout)


def methods(source, classname, names):
    cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == classname)
    nodes = [copy.deepcopy(n) for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == set(names)
    node = ast.ClassDef(name=classname, bases=[], keywords=[], body=nodes, decorator_list=[])
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
    return compile(ast.fix_missing_locations(module), "<live-source-contract>", "exec")


def live_enums():
    """Execute the real enum classes from schemas/types.py (not hand-written stand-ins)."""
    nodes = [n for n in ast.parse(sources["schemas/types.py"]).body
             if isinstance(n, ast.ClassDef) and n.name in {"TorrentStatus", "TorrentQueryStatus", "DownloadTaskState"}]
    scope = {"Enum": Enum}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), "<live-enums>", "exec"), scope)
    return scope


enums = live_enums()
Query = enums["TorrentQueryStatus"]


raw = [dict(hash="a" * 40, name="owned", tags="mp-qb115", progress=1, amount_left=0, state="uploading", save_path="/isolated"),
       dict(hash="b" * 40, name="ordinary", tags="MOVIEPILOT", progress=1, amount_left=0, state="uploading", save_path="/ordinary")]
logger = SimpleNamespace(error=lambda *a: None)
ns = dict(logger=logger)
exec(methods(sources[PATHS[1]], "Qbittorrent", ["get_torrents", "get_completed_torrents"]), ns)
client = ns["Qbittorrent"]()
client.qbc = SimpleNamespace(torrents_info=lambda **kw: copy.deepcopy(raw))
assert [t["name"] for t in client.get_completed_torrents(tags="MOVIEPILOT")] == ["ordinary"]
assert [t["name"] for t in client.get_completed_torrents(tags="mp-qb115")] == ["owned"]
ns.update(Path=Path, TorrentQueryStatus=Query, TorrentStatus=enums["TorrentStatus"], DownloadTaskState=enums["DownloadTaskState"],
          get_runtime_setting=lambda key: "MOVIEPILOT", DownloaderTorrent=lambda **kw: SimpleNamespace(**kw),
          MetaInfo=lambda title: SimpleNamespace(name=title, year=None, season_episode=None),
          size_tools=SimpleNamespace(format_compact_size=lambda n: str(n)),
          time_tools=SimpleNamespace(format_duration=lambda n: str(n)))
for node in ast.parse(sources[PATHS[0]]).body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith("_QBITTORRENT_") for t in node.targets):
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<live-constants>", "exec"), ns)
exec(methods(sources[PATHS[0]], "QbittorrentModule", ["list_torrents", "__normalize_torrent_state"]), ns)
exec(methods(sources[PATHS[2]], "_DownloaderModuleBase", ["_normalize_query_status"]), ns)
module = ns["QbittorrentModule"]()
module._normalize_query_status = ns["_DownloaderModuleBase"]._normalize_query_status
module.get_instances = lambda: {"physical": client}
module.get_instance = lambda name: client
module.normalize_return_path = lambda path, name: str(path)
assert [t.title for t in module.list_torrents(status="transfer")] == ["ordinary"]
assert {t.title for t in module.list_torrents(status="transfer", include_all_tags=True)} == {"ordinary", "owned"}
queue = ast.parse(sources[PATHS[3]])
calls = [n for n in ast.walk(queue) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
         and n.func.attr == "list_torrents"]
transfer = [c for c in calls if any(k.arg == "status" and isinstance(k.value, ast.Attribute)
                                   and k.value.attr == "TRANSFER" for k in c.keywords)]
assert transfer and all(not any(k.arg == "include_all_tags" for k in c.keywords) for c in transfer)
base = ast.parse(sources[PATHS[4]])
required = {n.name for n in ast.walk(base) if isinstance(n, ast.FunctionDef)
            and any(isinstance(d, ast.Name) and d.id == "abstractmethod" for d in n.decorator_list)}
plugin = ast.parse((Path(__file__).resolve().parents[1] / "__init__.py").read_text())
implemented = {n.name for n in ast.walk(plugin) if isinstance(n, ast.FunctionDef)}
assert required.issubset(implemented), required - implemented
print("PASS live source: QbittorrentModule; real native tag filtering excludes mp-qb115 from automatic TRANSFER")
print("PASS live source: transfer queue does not request include_all_tags; explicit all-tags manual query does include it")
print("PASS live source: all current SDK abstract plugin hooks implemented")


# ---- review round 2: contracts the plugin relies on beyond tag filtering ---------------------------------
def tree(path):
    return ast.parse(sources[path])


def find_class(module, name):
    return next(n for n in module.body if isinstance(n, ast.ClassDef) and n.name == name)


def find_func(owner, name):
    return next(n for n in ast.walk(owner) if isinstance(n, ast.FunctionDef) and n.name == name)


def arg_names(func):
    a = func.args
    return [x.arg for x in a.posonlyargs + a.args + a.kwonlyargs]


def enum_values(module, classname):
    return {n.targets[0].id: ast.literal_eval(n.value) for n in find_class(module, classname).body
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and isinstance(n.value, ast.Constant)}


plugin_cls = find_class(plugin, "Qb115Downloader")
plugin_sets = {t.id: ast.literal_eval(n.value) for n in plugin_cls.body if isinstance(n, ast.Assign)
               for t in n.targets if isinstance(t, ast.Name) and t.id in {"_TRANSFER", "_DOWNLOADING", "_COMPLETED", "_PAUSED"}}

# (c) already-tracked message must be MoviePilot's constant
live_exists = next(ast.literal_eval(n.value) for n in tree("schemas/download.py").body
                   if isinstance(n, ast.Assign) and n.targets[0].id == "DOWNLOAD_TASK_ALREADY_EXISTS")
ours = {ast.literal_eval(n.value) for n in ast.walk(plugin) if isinstance(n, ast.Assign) and n.targets
        and isinstance(n.targets[0], ast.Name) and n.targets[0].id == "DOWNLOAD_TASK_ALREADY_EXISTS"}
assert live_exists in ours, (live_exists, ours)
assert "error_msg == DOWNLOAD_TASK_ALREADY_EXISTS" in sources["chain/download/submission.py"]
print("PASS live source: DOWNLOAD_TASK_ALREADY_EXISTS equals plugin literal; submission keys 'already_exists' off it")

# (a) status aliases: the plugin's sets are normalized by MP's real function to the same bucket
status_values = enum_values(tree("schemas/types.py"), "TorrentStatus")
assert status_values["DOWNLOADING"] in plugin_sets["_DOWNLOADING"] and status_values["TRANSFER"] in plugin_sets["_TRANSFER"], status_values
for key, bucket in (("_TRANSFER", "TRANSFER"), ("_DOWNLOADING", "DOWNLOADING"), ("_COMPLETED", "COMPLETED"), ("_PAUSED", "PAUSED")):
    for alias in plugin_sets[key]:
        assert module._normalize_query_status(alias).name == bucket, (alias, bucket)
print("PASS live source: plugin status aliases (incl. Chinese TorrentStatus values) map to the same buckets as MP's normalizer")

# (d) downloading() asks for our exact downloader name with TorrentStatus.DOWNLOADING; dispatcher merge semantics
service = find_func(tree("application/download/tasks.py"), "downloading")
call = next(n for n in ast.walk(service) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "_list_torrents")
kw = {k.arg: k.value for k in call.keywords}
assert isinstance(kw["downloader"], ast.Name) and kw["downloader"].id == "name"
assert isinstance(kw["status"], ast.Attribute) and kw["status"].attr == "DOWNLOADING"
contracts = next(n for n in ast.walk(tree("runtime/extensions/module/contracts.py"))
                 if isinstance(n, (ast.Assign, ast.AnnAssign))
                 and getattr(n.target if isinstance(n, ast.AnnAssign) else n.targets[0], "id", "") == "_METHOD_CONTRACTS").value
aggregation, required = {}, {}
for key, value in zip(contracts.keys, contracts.values):
    if isinstance(key, ast.Constant) and key.value in {"list_torrents", "download", "stop_torrents", "start_torrents", "remove_torrents"}:
        fields = {k.arg: k.value for k in value.keywords}
        aggregation[key.value] = fields["aggregation"].attr
        required[key.value] = ast.literal_eval(fields["required_parameters"])
assert aggregation == {"list_torrents": "ORDERED_LIST_MERGE", "download": "FIRST_NON_EMPTY", "stop_torrents": "FIRST_NON_EMPTY",
                       "start_torrents": "FIRST_NON_EMPTY", "remove_torrents": "FIRST_NON_EMPTY"}, aggregation
exported = {n.value for n in ast.walk(find_func(plugin_cls, "get_module")) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
assert exported == set(aggregation), exported
for name, params in required.items():
    func = find_func(plugin_cls, name)
    assert func.args.kwarg or set(params) <= set(arg_names(func)), (name, params)
print("PASS live source: downloading() queries downloader=<name>, status=DOWNLOADING; list_torrents merges, others first-non-empty; plugin signatures cover required params")
print("NOTE downloader=None (global list/transfer) never reaches the plugin: it returns None for unspecified names by design")

# (b) actual download dir comes from list_torrents(hashs, downloader).save_path; native list gives the same fields
# submission.py uses Python 3.12 f-strings that the host 3.11 parser rejects: slice the text instead of AST.
text = sources["chain/download/submission.py"]
resolver = text[text.index("def _resolve_actual_download_dir"):]
resolver = resolver[:resolver.index("\n    def ", 10)]
assert "list_torrents(hashs=[download_hash], downloader=downloader)" in resolver and 'getattr(torrent, "save_path", None)' in resolver
print("PASS live source: download history path is read back from list_torrents(save_path); plugin fills save_path/content_path/path")

# private/native helpers the adapter calls directly
select = find_func(find_class(tree(PATHS[0]), "QbittorrentModule"), "_select_download_files")
assert arg_names(select) == ["server", "torrent_hash", "episodes", "custom_words"], arg_names(select)
assert any(isinstance(d, ast.Name) and d.id == "staticmethod" for d in select.decorator_list)
qb_cls = find_class(tree(PATHS[1]), "Qbittorrent")
add_torrent = find_func(qb_cls, "add_torrent")
assert add_torrent.args.kwarg and "ignore_category_check" in ast.get_source_segment(sources[PATHS[1]], add_torrent)
expected_params = {"add_torrent": {"content", "download_dir", "is_paused", "tag", "cookie", "category"},
                   "get_torrents": {"ids", "status", "tags"}, "stop_torrents": {"ids"}, "start_torrents": {"ids"},
                   "get_files": {"tid"}, "get_content_layout": set()}
for name, params in expected_params.items():
    assert params <= set(arg_names(find_func(qb_cls, name))), name
module_cls = find_class(tree(PATHS[0]), "QbittorrentModule")
helpers = {n.name: arg_names(n) for n in ast.walk(tree("modules/__init__.py")) if isinstance(n, ast.FunctionDef)
           and n.name in {"get_instance", "normalize_path", "normalize_return_path"}}
assert helpers.get("get_instance") == ["self", "name"], helpers
assert helpers.get("normalize_path") == ["self", "path", "downloader"], helpers
assert helpers.get("normalize_return_path") == ["self", "path", "downloader"], helpers
print("PASS live source: native _select_download_files (staticmethod) and qB instance/path helper signatures match the adapter's calls")

# known hazards the plugin only mitigates (README): keep asserting they still exist upstream
native_download = ast.get_source_segment(sources[PATHS[0]], find_func(module_cls, "download"))
assert "set_torrents_tag(ids=torrent_hash, tags=[get_runtime_setting('TORRENT_TAG')])" in native_download
remove = find_func(module_cls, "remove_torrents")
assert ast.literal_eval(remove.args.defaults[0]) is True  # delete_file defaults to True
assert "self.remove_torrents(\n                                t.download_hash, downloader=t.downloader" in sources["chain/transfer/settlement.py"]
print("PASS live source (hazard still present => README manual steps apply): native duplicate branch tags TORRENT_TAG; move mode removes with delete_file=True")


# ---- review round 2 (item 7): the reversible untag mitigation + why pause alone cannot stop native TRANSFER ----
remove_tag = find_func(qb_cls, "remove_torrents_tag")
assert arg_names(remove_tag) == ["self", "ids", "tag"], arg_names(remove_tag)  # adapter calls (ids=..., tag=[...])
remove_tag_text = ast.get_source_segment(sources[PATHS[1]], remove_tag)
assert "torrents_remove_tags(torrent_hashes=ids, tags=tag)" in remove_tag_text  # tag removal only, nothing deleted
assert "delete" not in remove_tag_text.lower().replace("删除种子tag", "")
assert "torrents_info(torrent_hashes=ids" in ast.get_source_segment(sources[PATHS[1]], find_func(qb_cls, "get_torrents"))
completed_text = ast.get_source_segment(sources[PATHS[1]], find_func(qb_cls, "get_completed_torrents"))
assert 'status="completed"' in completed_text
for state in ("pausedup", "stoppedup", "pauseddl", "stoppeddl"):
    assert '"%s"' % state not in completed_text, state  # paused/stopped completed torrents are NOT excluded
print("PASS live source: Qbittorrent.remove_torrents_tag(ids, tag) only removes tags; get_completed_torrents does not exclude pausedUP/stoppedUP")


# ---- review round 3: facts behind the registry write, the ledger location and the get_state/service gating ----
oper = find_class(tree("db/oper/systemconfig.py"), "SystemConfigOper")
atomic = find_func(oper, "update_atomically")
assert arg_names(atomic) == ["self", "key", "mutation"], arg_names(atomic)  # plugin passes (key, mutation)
atomic_text = ast.get_source_segment(sources["db/oper/systemconfig.py"], atomic)
assert "mutation(db, current)" in atomic_text and "with self._write_lock" in atomic_text and "with_for_update" in atomic_text
assert enum_values(tree("schemas/types.py"), "SystemConfigKey")["Downloaders"] == "Downloaders"
print("PASS live source: SystemConfigOper.update_atomically(key, mutation(session, current)->(result, value)) under the config write lock; key Downloaders")

model = find_class(tree("db/models/plugindata.py"), "PluginData")
columns = {n.target.id for n in model.body if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)}
assert {"plugin_id", "key", "value"} <= columns, columns
base_save = ast.get_source_segment(sources["sdk/plugin/base.py"], find_func(find_class(tree("sdk/plugin/base.py"), "_PluginBase"), "save_data"))
assert "plugin_id = self.__class__.__name__" in base_save and "self.plugindata.save(plugin_id, key, value)" in base_save
assert "return str(cls.__name__).lower()" in sources["db/base.py"]  # Base.__tablename__ -> "plugindata"
print("PASS live source: ledger = table plugindata, row (plugin_id='Qb115Downloader' [class name], key='tasks_v1')")

services = ast.get_source_segment(sources["runtime/extensions/plugin/projection.py"], find_func(tree("runtime/extensions/plugin/projection.py"), "services"))
assert "if plugin.get_state():" in services and "plugin.get_service()" in services
print("PASS live source: plugin services are registered only while get_state() is true (guard-only plugins must report True)")
