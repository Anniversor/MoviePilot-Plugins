"""Read-only CLM/MP transfer-history mapping; never infer identity from a media title."""
import json
from pathlib import PurePosixPath


def resolve_history(task, item, lookup, mount_prefix, openlist_base):
    def safe(path):
        if not isinstance(path, str) or not path.startswith('/') or any(p in {'', '.', '..'} for p in path[1:].split('/')) or '\\' in path:
            raise ValueError('unsafe history path')
        return path
    mount_prefix, openlist_base = safe(mount_prefix), safe(openlist_base)
    if openlist_base != '/115' or mount_prefix in {'/115', '/'}:
        raise ValueError('unsupported history mapping')
    source = safe(task['target'] + '/' + item['remote'])
    if not source.startswith(openlist_base + '/'):
        raise ValueError('history source outside storage')
    src = mount_prefix + source[len(openlist_base):]
    row = lookup(src, 'local')
    if (row is None or row.status is not True or row.src != src or row.src_storage != 'local'
            or row.dest_storage != 'local'):
        raise ValueError('exact successful history unavailable')
    for key in ('src_fileitem', 'dest_fileitem'):
        value = getattr(row, key, None)
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict) or type(value.get('size')) is not int or value['size'] != item['size']:
            raise ValueError('history file size mismatch')
    dest = safe(row.dest)
    if not dest.startswith(mount_prefix + '/') or dest == src:
        raise ValueError('history final mapping outside cloud')
    return openlist_base + dest[len(mount_prefix):]
