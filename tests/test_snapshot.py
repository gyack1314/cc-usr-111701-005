"""受控站点快照采集：覆盖证明、显式状态与引用闭合。"""
import json
import subprocess
import sys
import unittest
from pathlib import Path

from capture_data.journal import connect
from capture_data.siteview import raw_observations, site_view
from capture_data.snapshot import (SnapshotCollector, SnapshotStore, build_url,
                                   entity_url, version_of)
from capture_data.transport import AuthGuard, CrossOriginRequestBlocked, ReplayTransport

BASE = 'https://sensor.example.test/v1.1'
ROOT = Path(__file__).resolve().parents[1]

THING = {'@iot.id': 1, 'name': 'alpha', 'description': 'synthetic'}
SENSOR = {'@iot.id': 7, 'name': 'thermometer', 'metadata': 'v1'}
OBSERVED = {'@iot.id': 5, 'name': 'temperature'}
DATASTREAM = {'@iot.id': 3, 'name': 'ds-temp', 'Thing@iot.navigationLink': 'Things(1)',
              'Sensor': SENSOR, 'ObservedProperty': OBSERVED}
OBS11 = {'@iot.id': 11, 'phenomenonTime': '2026-11-17T01:00:00Z', 'result': 18.2,
         'Datastream@iot.navigationLink': 'Datastreams(3)'}
OBS12 = {'@iot.id': 12, 'phenomenonTime': '2026-11-17T02:00:00Z', 'result': 18.6,
         'Datastream@iot.navigationLink': 'Datastreams(3)'}
OBS13 = {'@iot.id': 13, 'phenomenonTime': '2026-11-17T03:00:00Z', 'result': 18.9,
         'Datastream@iot.navigationLink': 'Datastreams(3)'}

THINGS_URL = build_url(BASE, 'Things', orderby='@iot.id asc', top=100)
DATASTREAMS_URL = build_url(BASE, 'Datastreams', expand='Sensor,ObservedProperty',
                            orderby='@iot.id asc', top=100)
OBSERVATIONS_URL = build_url(BASE, 'Observations', orderby='phenomenonTime asc', top=100)
PAGE2_URL = BASE + '/Observations?$skip=2'
RESCAN_URL = build_url(BASE, 'Observations',
                       filter_='phenomenonTime ge 2026-11-17T02:00:00Z',
                       orderby='phenomenonTime asc', top=100)
VERIFY_DS_URL = entity_url(BASE, 'Datastreams', '3', expand='Sensor,ObservedProperty')
VERIFY_THING_URL = entity_url(BASE, 'Things', '1')
DS_OWN = {k: v for k, v in DATASTREAM.items() if k not in ('Sensor', 'ObservedProperty')}


def page(value, next_link=None):
    body = {'value': value}
    if next_link:
        body['@iot.nextLink'] = next_link
    return {'status': 200, 'body': body}


def call(url, response):
    return {'request': {'method': 'GET', 'url': url}, 'response': response}


def inventory_calls(datastream=None):
    return [call(THINGS_URL, page([THING])),
            call(DATASTREAMS_URL, page([datastream or DATASTREAM]))]


def verify_calls(datastream=None):
    return [call(VERIFY_DS_URL, {'status': 200, 'body': datastream or DATASTREAM}),
            call(VERIFY_THING_URL, {'status': 200, 'body': THING})]


def run_snapshot(store, calls):
    transport = ReplayTransport(calls)
    collector = SnapshotCollector(transport, store, BASE, 'site-07')
    return collector.run(), transport


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect(':memory:')
        self.store = SnapshotStore(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_happy_path_closes_site(self):
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12], next_link=PAGE2_URL)),
                    call(PAGE2_URL, page([OBS13]))]
                 + verify_calls())
        summary, _ = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'closed')
        site = site_view(self.store, 'site-07')
        self.assertEqual(site['status'], 'closed')
        self.assertEqual(set(site['entities']['things']), {'1'})
        self.assertEqual(set(site['entities']['datastreams']), {'3'})
        self.assertEqual(set(site['entities']['sensors']), {'7'})
        self.assertEqual(set(site['entities']['observed_properties']), {'5'})
        self.assertEqual([o['id'] for o in site['observations']], ['11', '12', '13'])
        ds_version = site['entities']['datastreams']['3']['version']
        self.assertEqual(ds_version, version_of(DS_OWN))
        for obs in site['observations']:
            self.assertEqual(obs['entity_versions']['datastream'], ds_version)
            self.assertFalse(obs['pin_drift'])
        self.assertFalse(site['entity_version_drift'])
        self.assertEqual(site['late_observations'], [])

    def test_cross_origin_next_link_not_followed(self):
        foreign = 'https://mirror.other.test/v1.1/Observations?$skip=2'
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12], next_link=foreign))]
                 + verify_calls())
        guarded = AuthGuard(ReplayTransport(calls), BASE, token='site-secret')
        summary = SnapshotCollector(guarded, self.store, BASE, 'site-07').run()
        self.assertEqual(summary['status'], 'open')
        self.assertTrue(any('跨源' in r for r in summary['reasons']))
        urls = [r['url'] for r in guarded.transport.requests]
        self.assertNotIn(foreign, urls)
        self.assertTrue(all('mirror.other.test' not in u for u in urls))
        for req in guarded.transport.requests:
            self.assertEqual(req['headers'].get('Authorization'), 'Bearer site-secret')
        site = site_view(self.store, 'site-07')
        self.assertEqual(site['status'], 'incomplete')
        self.assertEqual(site['observation_count'], 2)
        raw = raw_observations(self.store, 'site-07')
        self.assertEqual({r['id'] for r in raw}, {'11', '12'})

    def test_auth_guard_blocks_cross_origin(self):
        guard = AuthGuard(ReplayTransport([]), BASE, token='secret')
        with self.assertRaises(CrossOriginRequestBlocked):
            guard.request('GET', 'https://mirror.other.test/v1.1/Observations')

    def test_token_expiry_rescans_provable_range(self):
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12], next_link=PAGE2_URL)),
                    call(PAGE2_URL, {'status': 410, 'body': {'error': 'token expired'}}),
                    call(RESCAN_URL, page([OBS12, OBS13]))]
                 + verify_calls())
        summary, transport = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'closed')
        self.assertIn(RESCAN_URL, [r['url'] for r in transport.requests])
        self.assertTrue(any(e['kind'] == 'token_expired' for e in summary['events']))
        site = site_view(self.store, 'site-07')
        self.assertEqual([o['id'] for o in site['observations']], ['11', '12', '13'])
        raw = raw_observations(self.store, 'site-07')
        self.assertEqual(len([r for r in raw if r['id'] == '12']), 1)

    def test_unrecoverable_token_expiry_stays_open(self):
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12], next_link=PAGE2_URL)),
                    call(PAGE2_URL, {'status': 410, 'body': {'error': 'token expired'}})])
        summary, _ = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'open')
        self.assertTrue(any('覆盖未闭合' in r for r in summary['reasons']))
        site = site_view(self.store, 'site-07')
        self.assertEqual(site['status'], 'incomplete')
        self.assertEqual(site['observation_count'], 2)

    def test_duplicate_id_conflict_blocks_closure(self):
        changed = dict(OBS12, result=19.9)
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12], next_link=PAGE2_URL)),
                    call(PAGE2_URL, {'status': 410, 'body': {'error': 'token expired'}}),
                    call(RESCAN_URL, page([changed, OBS13]))]
                 + verify_calls())
        summary, _ = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'open')
        self.assertTrue(any('同 ID 不同内容' in r for r in summary['reasons']))
        raw = [r for r in raw_observations(self.store, 'site-07') if r['id'] == '12']
        self.assertEqual(len(raw), 2)
        self.assertEqual({r['status'] for r in raw}, {'id_conflict'})
        self.assertEqual(site_view(self.store, 'site-07')['status'], 'incomplete')

    def test_late_observation_marked(self):
        first = inventory_calls() + [call(OBSERVATIONS_URL, page([OBS11, OBS12]))] + verify_calls()
        summary1, _ = run_snapshot(self.store, first)
        self.assertEqual(summary1['status'], 'closed')
        late = {'@iot.id': 10, 'phenomenonTime': '2026-11-17T00:30:00Z', 'result': 17.9,
                'Datastream@iot.navigationLink': 'Datastreams(3)'}
        second = (inventory_calls()
                  + [call(OBSERVATIONS_URL, page([late, OBS11, OBS12]))]
                  + verify_calls())
        summary2, _ = run_snapshot(self.store, second)
        self.assertEqual(summary2['status'], 'closed')
        site = site_view(self.store, 'site-07')
        self.assertEqual(site['late_observations'], ['10'])
        raw = {r['id']: r for r in raw_observations(self.store, 'site-07')}
        self.assertEqual(raw['10']['status'], 'late')
        self.assertEqual(raw['11']['status'], 'ok')

    def test_deleted_observation_tombstoned(self):
        first = inventory_calls() + [call(OBSERVATIONS_URL, page([OBS11, OBS12]))] + verify_calls()
        run_snapshot(self.store, first)
        second = (inventory_calls()
                  + [call(OBSERVATIONS_URL, page([OBS12]))]
                  + verify_calls())
        summary2, _ = run_snapshot(self.store, second)
        self.assertEqual(summary2['status'], 'closed')
        site = site_view(self.store, 'site-07')
        self.assertEqual([o['id'] for o in site['observations']], ['12'])
        self.assertEqual(site['deleted_observation_count'], 1)
        raw = {r['id']: r for r in raw_observations(self.store, 'site-07')}
        self.assertEqual(raw['11']['status'], 'deleted')

    def test_entity_version_change_mid_run_blocks(self):
        changed = dict(DATASTREAM, name='ds-temp-v2')
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([OBS11, OBS12]))]
                 + verify_calls(datastream=changed))
        summary, _ = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'open')
        self.assertTrue(any('实体版本发生变化' in r for r in summary['reasons']))
        original = version_of(DS_OWN)
        for r in raw_observations(self.store, 'site-07'):
            self.assertEqual(r['entity_versions']['datastream'], original)
        self.assertEqual(site_view(self.store, 'site-07')['status'], 'incomplete')

    def test_entity_version_change_across_runs_disclosed(self):
        first = inventory_calls() + [call(OBSERVATIONS_URL, page([OBS11, OBS12]))] + verify_calls()
        summary1, _ = run_snapshot(self.store, first)
        self.assertEqual(summary1['status'], 'closed')
        changed = dict(DATASTREAM, name='ds-temp-v2')
        second = (inventory_calls(datastream=changed)
                  + [call(OBSERVATIONS_URL, page([OBS11, OBS12]))]
                  + verify_calls(datastream=changed))
        summary2, _ = run_snapshot(self.store, second)
        self.assertEqual(summary2['status'], 'closed')
        site = site_view(self.store, 'site-07')
        self.assertTrue(site['entity_version_drift'])
        old_version = version_of(DS_OWN)
        new_version = version_of({k: v for k, v in changed.items()
                                  if k not in ('Sensor', 'ObservedProperty')})
        self.assertEqual(site['entities']['datastreams']['3']['version'], new_version)
        for obs in site['observations']:
            self.assertEqual(obs['entity_versions']['datastream'], old_version)
            self.assertTrue(obs['pin_drift'])

    def test_unclosed_refs_block_site(self):
        orphan = {'@iot.id': 21, 'phenomenonTime': '2026-11-17T01:30:00Z', 'result': 20.1,
                  'Datastream@iot.navigationLink': 'Datastreams(9)'}
        calls = (inventory_calls()
                 + [call(OBSERVATIONS_URL, page([orphan]))]
                 + verify_calls())
        summary, _ = run_snapshot(self.store, calls)
        self.assertEqual(summary['status'], 'open')
        self.assertTrue(any('引用未闭合' in r for r in summary['reasons']))
        site = site_view(self.store, 'site-07')
        self.assertEqual(site['status'], 'incomplete')
        raw = raw_observations(self.store, 'site-07')
        self.assertEqual([r['id'] for r in raw], ['21'])

    def test_cli_snapshot_manifests(self):
        for manifest, expected in (('snapshot-ok.json', 'closed'),
                                   ('snapshot-token.json', 'closed'),
                                   ('snapshot-foreign.json', 'incomplete')):
            proc = subprocess.run(
                [sys.executable, '-m', 'capture_data.cli', 'snapshot',
                 str(ROOT / 'data' / manifest)],
                capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            out = json.loads(proc.stdout)
            self.assertEqual(out['site']['status'], expected, manifest)
            if manifest == 'snapshot-foreign.json':
                self.assertTrue(any('跨源' in r for r in out['runs'][0]['reasons']))
                self.assertEqual(len(out['raw_observations']), 2)


if __name__ == '__main__':
    unittest.main()
