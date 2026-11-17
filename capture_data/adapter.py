import json
from urllib.parse import urljoin
def decode(raw, manifest):
    doc=json.loads(raw.decode('utf-8'));next_link=doc.get('@iot.nextLink')
    records=[{'kind':'thing','id':manifest['source_url'],'version':'received','refs':[],
              'payload':{'next_link':next_link}},
             {'kind':'datastream','id':'Datastreams(3)','version':'received','refs':[manifest['source_url']],
              'payload':{'source_url':manifest['source_url']}}]
    for obs in doc['value']:
        link=obs['Datastream@iot.navigationLink']
        records.append({'kind':'observation','id':str(obs['@iot.id']),'version':obs['phenomenonTime'],
                        'refs':[link],'payload':{'result':obs['result'],'navigation_link':link}})
    records.append({'kind':'sensor','id':'sensor-source','version':'received','refs':['Datastreams(3)'],
                    'payload':{'source_url':manifest['source_url']}})
    return {'protocol':manifest['protocol'],'collection':manifest['collection'],'records':records}
