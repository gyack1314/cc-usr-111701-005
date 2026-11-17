"""站点视图与原始观测查询：引用未闭合时不组装站点，原始观测始终可查。"""
import json

_KINDS = ('thing', 'datastream', 'sensor', 'observed_property')
_PLURAL = {'thing': 'things', 'datastream': 'datastreams',
           'sensor': 'sensors', 'observed_property': 'observed_properties'}


def raw_observations(store, collection):
    """原始观测（含全部状态），不受站点闭合状态影响。"""
    out = []
    for r in store.observations(collection):
        out.append({'id': r['obs_id'], 'version': r['version'],
                    'phenomenon_time': r['phenomenon_time'],
                    'datastream_id': r['datastream_id'], 'status': r['status'],
                    'entity_versions': json.loads(r['pins']),
                    'first_run': r['first_run'], 'content': json.loads(r['content'])})
    return out


def site_view(store, collection):
    """仅在最近一次运行闭合时组装站点；否则只给原因，不给伪完整站点。"""
    run = store.latest_run(collection)
    if run is None:
        return {'status': 'no_runs', 'collection': collection}
    if run['status'] != 'closed':
        return {'status': 'incomplete', 'collection': collection, 'run_id': run['run_id'],
                'reasons': json.loads(run['reasons']),
                'observation_count': len(store.observations(collection)),
                'last_closed_run': store.latest_closed_run(collection)}
    entities = {}
    for kind in _KINDS:
        entities[_PLURAL[kind]] = {
            r['entity_id']: {'version': r['version'], 'content': json.loads(r['content'])}
            for r in store.entities(collection, kind)
            if r['status'] == 'ok' and r['last_run'] == run['run_id']}
    observations = []
    late = []
    drift = False
    for r in store.observations(collection):
        if r['status'] in ('deleted', 'id_conflict'):
            continue
        pins = json.loads(r['pins'])
        current = entities['datastreams'].get(r['datastream_id'])
        pin_drift = bool(pins.get('datastream') and current
                         and pins['datastream'] != current['version'])
        drift = drift or pin_drift
        observations.append({'id': r['obs_id'], 'version': r['version'],
                             'phenomenon_time': r['phenomenon_time'],
                             'datastream_id': r['datastream_id'], 'status': r['status'],
                             'entity_versions': pins, 'pin_drift': pin_drift,
                             'content': json.loads(r['content'])})
        if r['status'] == 'late':
            late.append(r['obs_id'])
    deleted = sum(1 for r in store.observations(collection) if r['status'] == 'deleted')
    return {'status': 'closed', 'collection': collection, 'run_id': run['run_id'],
            'entities': entities, 'observations': observations,
            'late_observations': late, 'deleted_observation_count': deleted,
            'entity_version_drift': drift,
            'coverage': [{'lo': lo, 'hi': hi, 'reached_end': reached}
                         for lo, hi, reached in store.merged_intervals(collection, 'observations')]}
