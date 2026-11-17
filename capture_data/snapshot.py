"""受控站点快照采集：可证明覆盖、显式状态、引用闭合。

身份约定：kind + @iot.id 定位实体；实体版本为规范化 JSON 的 SHA-256。
每条观测记录其采集时刻所属 Datastream/Thing/Sensor/ObservedProperty 的版本。

可证明覆盖：翻页流要求服务端排序（观测 phenomenonTime，其余 @iot.id）。
分页令牌失效时，从最后已见键以 ge 条件含边界重扫；只有拿到无 nextLink
的末页，覆盖区间才闭合——页数本身不构成完整性证据。

跨源 nextLink 不跟随（凭据不出源）；同 ID 不同内容、删除、晚到观测都有
显式状态；引用未闭合时原始观测仍可查询，但不组装站点视图。
"""
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import quote, urljoin

from .transport import origin_of

PROTOCOL = 'sensorthings-api'

SCHEMA = """
CREATE TABLE IF NOT EXISTS snap_runs(
  run_id TEXT PRIMARY KEY,
  collection TEXT NOT NULL,
  base_url TEXT NOT NULL,
  window_start TEXT,
  window_end TEXT,
  started_utc TEXT NOT NULL,
  finished_utc TEXT,
  status TEXT NOT NULL,
  reasons TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS snap_events(
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT NOT NULL,
  PRIMARY KEY(run_id, seq)
);
CREATE TABLE IF NOT EXISTS snap_entities(
  collection TEXT NOT NULL,
  kind TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  version TEXT NOT NULL,
  content TEXT NOT NULL,
  first_run TEXT NOT NULL,
  last_run TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ok',
  PRIMARY KEY(collection, kind, entity_id, version)
);
CREATE TABLE IF NOT EXISTS snap_observations(
  collection TEXT NOT NULL,
  obs_id TEXT NOT NULL,
  version TEXT NOT NULL,
  phenomenon_time TEXT NOT NULL,
  datastream_id TEXT,
  pins TEXT NOT NULL,
  content TEXT NOT NULL,
  first_run TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ok',
  PRIMARY KEY(collection, obs_id, version)
);
CREATE TABLE IF NOT EXISTS snap_coverage(
  collection TEXT NOT NULL,
  run_id TEXT NOT NULL,
  stream TEXT NOT NULL,
  lo_key TEXT,
  hi_key TEXT,
  reached_end INTEGER NOT NULL DEFAULT 0
);
"""

_NAV = re.compile(r'^([A-Za-z]+)\((.*)\)$')


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def version_of(obj):
    return hashlib.sha256(canon(obj).encode('utf-8')).hexdigest()


def norm_time(value):
    if not isinstance(value, str):
        raise ValueError('phenomenonTime 非法')
    text = value.strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        raise ValueError('phenomenonTime 缺少时区')
    moment = moment.astimezone(timezone.utc)
    if moment.microsecond:
        return moment.isoformat().replace('+00:00', 'Z')
    return moment.strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_nav(link):
    """从 Things(1) 或绝对导航地址解析 (集合名, id)。"""
    if not isinstance(link, str) or not link:
        return None
    tail = link.rstrip('/').rsplit('/', 1)[-1]
    m = _NAV.match(tail)
    if not m:
        return None
    raw = m.group(2)
    if len(raw) >= 2 and raw.startswith("'") and raw.endswith("'"):
        raw = raw[1:-1].replace("''", "'")
    return m.group(1), raw


def _key_literal(key):
    text = str(key)
    if text.isdigit():
        return text
    return "'" + text.replace("'", "''") + "'"


def build_url(base, name, expand=None, filter_=None, orderby=None, top=None):
    parts = []
    if expand:
        parts.append('$expand=' + expand)
    if filter_:
        parts.append('$filter=' + quote(filter_, safe="()',=@:"))
    if orderby:
        parts.append('$orderby=' + quote(orderby, safe="()',=@:"))
    if top:
        parts.append('$top=' + str(int(top)))
    return base.rstrip('/') + '/' + name + ('?' + '&'.join(parts) if parts else '')


def entity_url(base, name, entity_id, expand=None):
    return build_url(base, name + '(' + _key_literal(entity_id) + ')', expand=expand)


def interval_contains(interval, key):
    lo, hi, _ = interval
    return (lo is None or key >= lo) and (hi is None or key <= hi)


def _merge_intervals(rows):
    ordered = sorted(rows, key=lambda r: (r[0] is not None, r[0] or ''))
    merged = []
    for lo, hi, reached in ordered:
        if merged:
            plo, phi, preached = merged[-1]
            if phi is None or lo is None or lo <= phi:
                nhi = None if phi is None or hi is None else max(phi, hi)
                merged[-1] = (plo, nhi, preached or reached)
                continue
        merged.append((lo, hi, reached))
    return merged


class SnapshotStore:
    """快照账本：运行、事件、实体版本、观测与覆盖区间。"""

    def __init__(self, conn):
        self.conn = conn
        conn.executescript(SCHEMA)

    @staticmethod
    def _dicts(cur):
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _write(self, sql, args):
        with self.conn:
            self.conn.execute(sql, args)

    def _collection_of(self, run_id):
        row = self.conn.execute('SELECT collection FROM snap_runs WHERE run_id=?', (run_id,)).fetchone()
        if not row:
            raise ValueError('未知运行')
        return row[0]

    def begin_run(self, collection, base_url, window_start, window_end):
        run_id = uuid.uuid4().hex[:12]
        self._write(
            'INSERT INTO snap_runs(run_id,collection,base_url,window_start,window_end,started_utc,status)'
            ' VALUES(?,?,?,?,?,?,?)',
            (run_id, collection, base_url, window_start, window_end,
             datetime.now(timezone.utc).isoformat(), 'running'))
        return run_id

    def finish_run(self, run_id, status, reasons):
        self._write('UPDATE snap_runs SET finished_utc=?,status=?,reasons=? WHERE run_id=?',
                    (datetime.now(timezone.utc).isoformat(), status,
                     json.dumps(reasons, ensure_ascii=False), run_id))

    def latest_run(self, collection):
        rows = self._dicts(self.conn.execute(
            'SELECT * FROM snap_runs WHERE collection=? ORDER BY rowid DESC LIMIT 1', (collection,)))
        return rows[0] if rows else None

    def latest_closed_run(self, collection):
        rows = self._dicts(self.conn.execute(
            "SELECT run_id FROM snap_runs WHERE collection=? AND status='closed'"
            " ORDER BY rowid DESC LIMIT 1", (collection,)))
        return rows[0]['run_id'] if rows else None

    def add_event(self, run_id, kind, detail):
        with self.conn:
            seq = self.conn.execute(
                'SELECT COUNT(*) FROM snap_events WHERE run_id=?', (run_id,)).fetchone()[0] + 1
            self.conn.execute('INSERT INTO snap_events VALUES(?,?,?,?)',
                              (run_id, seq, kind, json.dumps(detail, ensure_ascii=False, sort_keys=True)))

    def events(self, run_id):
        rows = self._dicts(self.conn.execute(
            'SELECT * FROM snap_events WHERE run_id=? ORDER BY seq', (run_id,)))
        for row in rows:
            row['detail'] = json.loads(row['detail'])
        return rows

    def put_entity(self, run_id, kind, entity_id, content):
        """按约定身份保存实体版本；返回 (版本, 是否同 ID 已有其他版本)。"""
        collection = self._collection_of(run_id)
        version = version_of(content)
        row = self.conn.execute(
            'SELECT status FROM snap_entities WHERE collection=? AND kind=? AND entity_id=? AND version=?',
            (collection, kind, entity_id, version)).fetchone()
        if row:
            self._write(
                'UPDATE snap_entities SET last_run=?,status=?'
                ' WHERE collection=? AND kind=? AND entity_id=? AND version=?',
                (run_id, 'ok', collection, kind, entity_id, version))
            if row[0] == 'deleted':
                self.add_event(run_id, 'entity_restored',
                               {'kind': kind, 'id': entity_id, 'version': version})
            return version, False
        had = self.conn.execute(
            'SELECT 1 FROM snap_entities WHERE collection=? AND kind=? AND entity_id=? LIMIT 1',
            (collection, kind, entity_id)).fetchone() is not None
        self._write('INSERT INTO snap_entities VALUES(?,?,?,?,?,?,?,?)',
                    (collection, kind, entity_id, version, canon(content), run_id, run_id, 'ok'))
        return version, had

    def entity_seen_this_run(self, collection, kind, entity_id, run_id):
        return self.conn.execute(
            'SELECT 1 FROM snap_entities'
            ' WHERE collection=? AND kind=? AND entity_id=? AND first_run=? LIMIT 1',
            (collection, kind, entity_id, run_id)).fetchone() is not None

    def put_observation(self, run_id, obs_id, content, phenomenon_time, datastream_id, pins):
        """保存观测；同 ID 不同内容并存为 id_conflict，晚到标记 late。"""
        collection = self._collection_of(run_id)
        version = version_of(content)
        rows = self._dicts(self.conn.execute(
            'SELECT * FROM snap_observations WHERE collection=? AND obs_id=?', (collection, obs_id)))
        late = self._is_late(run_id, collection, phenomenon_time)
        for row in rows:
            if row['version'] == version:
                if row['status'] == 'deleted':
                    status = 'late' if late else 'ok'
                    self._write(
                        'UPDATE snap_observations SET status=?'
                        ' WHERE collection=? AND obs_id=? AND version=?',
                        (status, collection, obs_id, version))
                    self.add_event(run_id, 'observation_restored',
                                   {'id': obs_id, 'phenomenon_time': phenomenon_time})
                return self._obs_status(collection, obs_id, version)
        if rows:
            status = 'id_conflict'
            self._write("UPDATE snap_observations SET status='id_conflict'"
                        ' WHERE collection=? AND obs_id=?', (collection, obs_id))
            self.add_event(run_id, 'id_conflict',
                           {'kind': 'observation', 'id': obs_id,
                            'versions': sorted({r['version'] for r in rows} | {version})})
        elif late:
            status = 'late'
            self.add_event(run_id, 'late_observation',
                           {'id': obs_id, 'phenomenon_time': phenomenon_time})
        else:
            status = 'ok'
        self._write('INSERT INTO snap_observations VALUES(?,?,?,?,?,?,?,?,?)',
                    (collection, obs_id, version, phenomenon_time, datastream_id,
                     json.dumps(pins, ensure_ascii=False, sort_keys=True),
                     canon(content), run_id, status))
        return status

    def _obs_status(self, collection, obs_id, version):
        row = self.conn.execute(
            'SELECT status FROM snap_observations WHERE collection=? AND obs_id=? AND version=?',
            (collection, obs_id, version)).fetchone()
        return row[0] if row else None

    def _is_late(self, run_id, collection, phenomenon_time):
        ivals = self.merged_intervals(collection, 'observations', exclude_run=run_id)
        return any(interval_contains(iv, phenomenon_time) for iv in ivals)

    def add_coverage(self, run_id, stream, lo_key, hi_key, reached_end):
        collection = self._collection_of(run_id)
        self._write('INSERT INTO snap_coverage VALUES(?,?,?,?,?,?)',
                    (collection, run_id, stream, lo_key, hi_key, 1 if reached_end else 0))

    def merged_intervals(self, collection, stream, run_id=None, exclude_run=None):
        sql = 'SELECT lo_key,hi_key,reached_end FROM snap_coverage WHERE collection=? AND stream=?'
        args = [collection, stream]
        if run_id is not None:
            sql += ' AND run_id=?'
            args.append(run_id)
        if exclude_run is not None:
            sql += ' AND run_id!=?'
            args.append(exclude_run)
        rows = [(r[0], r[1], bool(r[2])) for r in self.conn.execute(sql, args).fetchall()]
        return _merge_intervals(rows)

    def observations(self, collection, status=None):
        sql = 'SELECT * FROM snap_observations WHERE collection=?'
        args = [collection]
        if status:
            sql += ' AND status=?'
            args.append(status)
        sql += ' ORDER BY phenomenon_time, obs_id'
        return self._dicts(self.conn.execute(sql, args))

    def entities(self, collection, kind=None):
        sql = 'SELECT * FROM snap_entities WHERE collection=?'
        args = [collection]
        if kind:
            sql += ' AND kind=?'
            args.append(kind)
        sql += ' ORDER BY kind, entity_id, version'
        return self._dicts(self.conn.execute(sql, args))

    def live_entity_ids(self, collection, kind):
        return {r[0] for r in self.conn.execute(
            "SELECT DISTINCT entity_id FROM snap_entities WHERE collection=? AND kind=? AND status='ok'",
            (collection, kind)).fetchall()}

    def has_live_entity(self, collection, kind, entity_id):
        return self.conn.execute(
            "SELECT 1 FROM snap_entities WHERE collection=? AND kind=? AND entity_id=? AND status='ok'"
            ' LIMIT 1', (collection, kind, entity_id)).fetchone() is not None

    def set_observation_status(self, collection, obs_id, version, status):
        self._write('UPDATE snap_observations SET status=? WHERE collection=? AND obs_id=? AND version=?',
                    (status, collection, obs_id, version))

    def mark_entity_deleted(self, collection, kind, entity_id):
        self._write("UPDATE snap_entities SET status='deleted'"
                    ' WHERE collection=? AND kind=? AND entity_id=?',
                    (collection, kind, entity_id))


class ScanAborted(Exception):
    """采集链中断：运行保持开放，不据此宣布完整。"""


class SnapshotCollector:
    """受控快照采集器：同源翻页、可证明重扫、实体版本核验。"""

    def __init__(self, transport, store, base_url, collection, window_start=None, window_end=None,
                 page_size=100, max_pages=1000, max_rescans=8):
        self.transport = transport
        self.store = store
        self.base_url = base_url.rstrip('/')
        self.collection = collection
        self.window_start = norm_time(window_start) if window_start else None
        self.window_end = norm_time(window_end) if window_end else None
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_rescans = max_rescans
        self.origin = origin_of(self.base_url)

    def run(self):
        run_id = self.store.begin_run(self.collection, self.base_url,
                                      self.window_start, self.window_end)
        self.run_id = run_id
        self.inventory = {'thing': {}, 'datastream': {}, 'sensor': {}, 'observed_property': {}}
        self.ds_links = {}
        self.refs = []
        self.seen_obs = set()
        try:
            self._scan_stream('things', 'Things', None, self._take_thing)
            self._scan_stream('datastreams', 'Datastreams', 'Sensor,ObservedProperty',
                              self._take_datastream)
            self._scan_stream('observations', 'Observations', None, self._take_observation,
                              windowed=True)
            self._verify_entities()
        except ScanAborted:
            pass
        except (ValueError, KeyError, TypeError) as exc:
            self._event('collect_error', {'error': str(exc)})
        status, reasons = self._finalize()
        return {'run_id': run_id, 'collection': self.collection, 'status': status,
                'reasons': reasons, 'events': self.store.events(run_id)}

    def _event(self, kind, detail):
        self.store.add_event(self.run_id, kind, detail)

    def _scan_stream(self, stream, name, expand, on_item, windowed=False):
        orderby = 'phenomenonTime asc' if stream == 'observations' else '@iot.id asc'
        base_filter = None
        if windowed:
            clauses = []
            if self.window_start:
                clauses.append('phenomenonTime ge ' + self.window_start)
            if self.window_end:
                clauses.append('phenomenonTime le ' + self.window_end)
            base_filter = ' and '.join(clauses) or None
        chain_lo = self.window_start if windowed else None
        last_key = None
        rescans = 0
        url = build_url(self.base_url, name, expand=expand, filter_=base_filter,
                        orderby=orderby, top=self.page_size)
        pages = 0
        while url:
            pages += 1
            if pages > self.max_pages:
                self._event('scan_aborted', {'stream': stream, 'reason': '页数超过上限', 'pages': pages})
                raise ScanAborted
            try:
                resp = self.transport.request('GET', url)
            except (EOFError, ValueError) as exc:
                self._coverage(stream, chain_lo, last_key, False)
                self._event('transport_error', {'stream': stream, 'url': url, 'error': str(exc)})
                raise ScanAborted
            status = resp.get('status', 200) if isinstance(resp, dict) else 200
            if status == 410 or (status in (400, 404) and ('$skip' in url or 'skiptoken' in url)):
                # 分页令牌失效：从最后已见键含边界重扫，范围可证明
                self._coverage(stream, chain_lo, last_key, False)
                self._event('token_expired', {'stream': stream, 'url': url, 'rescan_from': last_key})
                rescans += 1
                if rescans > self.max_rescans:
                    self._event('scan_aborted', {'stream': stream, 'reason': '重扫次数超过上限'})
                    raise ScanAborted
                chain_lo = last_key
                url = build_url(self.base_url, name, expand=expand,
                                filter_=self._rescan_filter(stream, base_filter, last_key),
                                orderby=orderby, top=self.page_size)
                continue
            if status != 200:
                self._coverage(stream, chain_lo, last_key, False)
                self._event('http_error', {'stream': stream, 'url': url, 'status': status})
                raise ScanAborted
            body = resp.get('body') if isinstance(resp, dict) else resp
            if isinstance(body, (str, bytes)):
                body = json.loads(body)
            if not isinstance(body, dict) or not isinstance(body.get('value') or [], list):
                self._event('protocol_error', {'stream': stream, 'url': url})
                raise ScanAborted
            for item in body.get('value') or []:
                on_item(item)
                last_key = self._key_of(stream, item)
            next_link = body.get('@iot.nextLink')
            if not next_link:
                self._coverage(stream, chain_lo, last_key, True)
                return
            next_link = urljoin(url, next_link)
            if origin_of(next_link) != self.origin:
                # 跨源续页：不跟随、不带凭据，分支保持开放
                self._coverage(stream, chain_lo, last_key, False)
                self._event('cross_origin_next_link', {'stream': stream, 'url': next_link})
                return
            url = next_link

    def _coverage(self, stream, chain_lo, last_key, reached_end):
        if last_key is None and not reached_end:
            if chain_lo is None:
                return
            hi = chain_lo
        elif last_key is None:
            hi = None
        else:
            hi = last_key
        self.store.add_coverage(self.run_id, stream, chain_lo, hi, reached_end)

    def _rescan_filter(self, stream, base_filter, last_key):
        if last_key is None:
            return base_filter
        if stream == 'observations':
            clause = 'phenomenonTime ge ' + last_key
        else:
            clause = '@iot.id ge ' + _key_literal(last_key)
        return '(' + base_filter + ') and (' + clause + ')' if base_filter else clause

    def _key_of(self, stream, item):
        if stream == 'observations':
            return norm_time(item['phenomenonTime'])
        return str(item['@iot.id'])

    def _ingest_entity(self, kind, entity, phase):
        entity_id = str(entity['@iot.id'])
        seen_this_run = self.store.entity_seen_this_run(self.collection, kind, entity_id, self.run_id)
        version, had = self.store.put_entity(self.run_id, kind, entity_id, entity)
        if had:
            blocking = phase == 'verify' or seen_this_run
            self._event('entity_version_changed',
                        {'kind': kind, 'id': entity_id, 'version': version,
                         'phase': phase, 'blocking': blocking})
        return entity_id, version

    def _take_thing(self, entity):
        tid, version = self._ingest_entity('thing', entity, phase='scan')
        self.inventory['thing'][tid] = version

    def _ingest_datastream(self, entity, phase):
        entity = dict(entity)
        sensor = entity.pop('Sensor', None)
        observed = entity.pop('ObservedProperty', None)
        did, version = self._ingest_entity('datastream', entity, phase)
        out = {'datastream': version}
        nav = parse_nav(entity.get('Thing@iot.navigationLink'))
        if nav and nav[0] == 'Things':
            out['thing_id'] = nav[1]
        if isinstance(sensor, dict):
            sid, sver = self._ingest_entity('sensor', sensor, phase)
            self.inventory['sensor'][sid] = sver
            out['sensor_id'] = sid
            out['sensor'] = sver
        if isinstance(observed, dict):
            oid, over = self._ingest_entity('observed_property', observed, phase)
            self.inventory['observed_property'][oid] = over
            out['observed_property_id'] = oid
            out['observed_property'] = over
        return did, out

    def _take_datastream(self, entity):
        did, versions = self._ingest_datastream(entity, phase='scan')
        self.inventory['datastream'][did] = versions['datastream']
        links = {}
        if versions.get('thing_id'):
            links['thing'] = versions['thing_id']
            self.refs.append(('thing', versions['thing_id']))
        if versions.get('sensor_id'):
            links['sensor'] = versions['sensor_id']
            self.refs.append(('sensor', versions['sensor_id']))
        if versions.get('observed_property_id'):
            links['observed_property'] = versions['observed_property_id']
            self.refs.append(('observed_property', versions['observed_property_id']))
        self.ds_links[did] = links

    def _take_observation(self, entity):
        oid = str(entity['@iot.id'])
        ptime = norm_time(entity['phenomenonTime'])
        nav = parse_nav(entity.get('Datastream@iot.navigationLink'))
        did = nav[1] if nav and nav[0] == 'Datastreams' else None
        links = self.ds_links.get(did, {})
        pins = {
            'datastream': self.inventory['datastream'].get(did),
            'thing': self.inventory['thing'].get(links.get('thing')),
            'sensor': self.inventory['sensor'].get(links.get('sensor')),
            'observed_property': self.inventory['observed_property'].get(links.get('observed_property')),
        }
        self.store.put_observation(self.run_id, oid, entity, ptime, did, pins)
        self.seen_obs.add(oid)
        if did:
            self.refs.append(('datastream', did))

    def _verify_entities(self):
        for did in sorted(self.inventory['datastream']):
            self._verify_datastream(did)
        for tid in sorted(self.inventory['thing']):
            self._verify_one('Things', 'thing', tid)

    def _fetch_entity(self, url, kind, entity_id):
        try:
            resp = self.transport.request('GET', url)
        except (EOFError, ValueError) as exc:
            self._event('transport_error', {'url': url, 'error': str(exc)})
            raise ScanAborted
        status = resp.get('status', 200) if isinstance(resp, dict) else 200
        if status == 404:
            self.store.mark_entity_deleted(self.collection, kind, entity_id)
            self._event('entity_deleted', {'kind': kind, 'id': entity_id, 'phase': 'verify'})
            return None
        if status != 200:
            self._event('http_error', {'url': url, 'status': status})
            raise ScanAborted
        body = resp.get('body') if isinstance(resp, dict) else resp
        if isinstance(body, (str, bytes)):
            body = json.loads(body)
        return body

    def _verify_datastream(self, did):
        url = entity_url(self.base_url, 'Datastreams', did, expand='Sensor,ObservedProperty')
        body = self._fetch_entity(url, 'datastream', did)
        if body is None:
            return
        _, versions = self._ingest_datastream(body, phase='verify')
        prior = self.ds_links.get(did, {})
        if prior.get('sensor') and 'sensor_id' not in versions:
            self._event('entity_version_changed',
                        {'kind': 'sensor', 'id': prior['sensor'], 'phase': 'verify',
                         'blocking': True, 'detail': '展开中消失'})
        if prior.get('observed_property') and 'observed_property_id' not in versions:
            self._event('entity_version_changed',
                        {'kind': 'observed_property', 'id': prior['observed_property'],
                         'phase': 'verify', 'blocking': True, 'detail': '展开中消失'})

    def _verify_one(self, name, kind, entity_id):
        url = entity_url(self.base_url, name, entity_id)
        body = self._fetch_entity(url, kind, entity_id)
        if body is None:
            return
        self._ingest_entity(kind, body, phase='verify')

    def _coverage_closed(self, stream):
        expected_lo = self.window_start if stream == 'observations' else None
        ivals = self.store.merged_intervals(self.collection, stream, run_id=self.run_id)
        return len(ivals) == 1 and ivals[0][2] and ivals[0][0] == expected_lo

    def _finalize(self):
        run_id = self.run_id
        ivals = self.store.merged_intervals(self.collection, 'observations', run_id=run_id)
        if ivals:
            for row in self.store.observations(self.collection):
                if row['status'] not in ('ok', 'late'):
                    continue
                if row['first_run'] == run_id or row['obs_id'] in self.seen_obs:
                    continue
                if any(interval_contains(iv, row['phenomenon_time']) for iv in ivals):
                    self.store.set_observation_status(self.collection, row['obs_id'],
                                                      row['version'], 'deleted')
                    self._event('observation_deleted',
                                {'id': row['obs_id'], 'phenomenon_time': row['phenomenon_time']})
        for stream, kind in (('things', 'thing'), ('datastreams', 'datastream')):
            if self._coverage_closed(stream):
                missing = self.store.live_entity_ids(self.collection, kind) - set(self.inventory[kind])
                for eid in sorted(missing):
                    self.store.mark_entity_deleted(self.collection, kind, eid)
                    self._event('entity_deleted', {'kind': kind, 'id': eid, 'phase': 'final'})
        unclosed = sorted({kind + '(' + eid + ')' for kind, eid in self.refs
                           if not self.store.has_live_entity(self.collection, kind, eid)})
        events = self.store.events(run_id)
        kinds = {e['kind'] for e in events}
        reasons = []
        if 'cross_origin_next_link' in kinds:
            reasons.append('存在跨源 nextLink，已拒绝跟随（凭据未外发）')
        if 'id_conflict' in kinds:
            reasons.append('存在同 ID 不同内容的记录')
        if any(e['kind'] == 'entity_version_changed' and e['detail'].get('blocking') for e in events):
            reasons.append('采集期间实体版本发生变化')
        if any(e['kind'] == 'entity_deleted' and e['detail'].get('phase') == 'verify' for e in events):
            reasons.append('采集期间实体被删除')
        for stream, label in (('observations', '观测'), ('things', 'Things'), ('datastreams', 'Datastreams')):
            if not self._coverage_closed(stream):
                reasons.append(label + '覆盖未闭合：不能仅凭页数推断完整')
        if unclosed:
            reasons.append('实体引用未闭合： ' + ', '.join(unclosed))
        status = 'closed' if not reasons else 'open'
        self.store.finish_run(run_id, status, reasons)
        return status, reasons
