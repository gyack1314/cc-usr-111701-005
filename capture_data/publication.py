"""单页观测不等于站点全集。"""
def assess(rows):
    pages=[r for r in rows if r['kind']=='thing' and r['payload'].get('next_link')]
    unresolved=[r['id'] for r in rows if r['kind']=='observation' and not r['payload'].get('entity_version_verified')]
    return {'publishable':not pages and not unresolved,
            'reasons':['还有续页:'+r['payload']['next_link'] for r in pages]+['观测实体版本待核:'+x for x in unresolved],
            'observation_count':len([r for r in rows if r['kind']=='observation'])}
