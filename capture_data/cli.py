"""查看已接收资料的身份与本地引用，并执行受控站点快照采集。"""
import argparse
import json
from pathlib import Path

from .domain import summarize
from .io import load
from .journal import connect, receive
from .projection import overview
from .siteview import raw_observations, site_view
from .snapshot import PROTOCOL, SnapshotCollector, SnapshotStore
from .transport import AuthGuard, ReplayTransport


def _inspect(path):
    manifest, digest = load(path)
    summary = summarize(manifest, path.parent)
    conn = connect(':memory:')
    source = receive(conn, manifest, path.parent)
    print(json.dumps({'manifest_sha256': digest, 'source_sha256': source, 'summary': summary,
                      'overview': overview(conn, manifest['collection'])},
                     ensure_ascii=False, indent=2))


def _snapshot(files, db_path):
    conn = connect(db_path) if db_path else connect(':memory:')
    store = SnapshotStore(conn)
    runs = []
    collection = None
    for name in files:
        manifest, _ = load(Path(name))
        if manifest.get('kind') != 'snapshot-calls' or manifest.get('protocol') != PROTOCOL:
            raise ValueError('清单不是受控快照呼叫记录')
        if collection is None:
            collection = manifest['collection']
        if manifest.get('collection') != collection:
            raise ValueError('同一命令内集合必须一致')
        window = manifest.get('window') or {}
        transport = AuthGuard(ReplayTransport(manifest['calls']), manifest['base_url'],
                              token=manifest.get('token'))
        collector = SnapshotCollector(transport, store, manifest['base_url'], collection,
                                      window_start=window.get('start'),
                                      window_end=window.get('end'))
        runs.append(collector.run())
    print(json.dumps({'runs': runs, 'site': site_view(store, collection),
                      'raw_observations': raw_observations(store, collection)},
                     ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    p_inspect = sub.add_parser('inspect')
    p_inspect.add_argument('file')
    p_snap = sub.add_parser('snapshot')
    p_snap.add_argument('files', nargs='+')
    p_snap.add_argument('--db', default=None)
    args = parser.parse_args()
    if args.command == 'inspect':
        _inspect(Path(args.file))
    else:
        _snapshot(args.files, args.db)


if __name__ == '__main__':
    main()
