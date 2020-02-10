import dateutil.parser
from datetime import datetime
import os, json, requests
from requests.auth import HTTPBasicAuth
import pandas as pd

os.chdir('/Users/adroesch/Documents/git/20-01_frost-data/air_temperature')

# get all available timeseries
a = requests.get('https://frost.met.no/observations/availableTimeSeries/v0.jsonld',
      params={
        'elements':'mean(air_temperature P1Y)'
      },
      auth=HTTPBasicAuth(os.environ['FROST_KEY'],os.environ['FROST_SECRET'])
)
sources = a.json()['data']
sources = [i for i in sources if i['sourceId'].split(':')[0][:2]=='SN'] #filter sensory system
len(sources)
pd.DataFrame(sources).to_csv('data/availableTimeSeries.csv',index=False)


# get observations
ids = list(set([i['sourceId'].split(':')[0] for i in sources]))
# ids = [i.strip() for i in open('data/failed_sources.txt','r').readlines()]
for i in ids:
    r = requests.get('https://frost.met.no/observations/v0.jsonld',
        params={
            'elements' : 'mean(air_temperature P1Y)',
            'sources': i,
            'referencetime':'1900-01-01T00:00:00Z/2020-01-01T00:00:00Z'
        },
        auth=HTTPBasicAuth(os.environ['FROST_KEY'],os.environ['FROST_SECRET'])
    )
    if r.status_code == 200:
        json.dump(r.json()['data'],open('data/observations/'+i+'.json','w'))
    else:
        with open('data/failed_sources.txt','a') as f:
            f.write(i+'\n')


# get sources meta
files = os.listdir('observations/')
ids = [i.split('.')[0] for i in files]
i = 0
metas = []
while (i*100) < len(ids):
    start = i*100
    end = (i*100)+100
    if end > len(ids):
        end = len(ids)
    idCsv = ','.join(ids[start:end])
    r = requests.get('https://frost.met.no/sources/v0.jsonld',
          params = {
            'ids': 'SN39500'
          },
          auth = HTTPBasicAuth( os.environ['FROST_KEY'], os.environ['FROST_SECRET'])
    )
    if r.status_code == 200:
        metas = metas + r.json()['data']
    else:
        print(i)
    i=i+1
len(metas)
json.dump(metas,open('data/files_metas.json','w'))
metas = json.load(open('data/files_metas.json','r'))


# clean data
metas = json.load(open('data/files_metas.json','r'))
d = {
    'sources': [],
    'observations': {}
}

fail_2019 = 0
fail_gap = 0
for i in metas:
    try:
        obs = json.load(open('data/observations/'+i['id']+'.json','r'))
        i['data_success'] = True
    except:
        i['data_success'] = False
        continue
    clean = [{
        'year': int(j['referenceTime'][:4]),
        'value': j['observations'][0]['value']
    } for j in obs]
    i['data_nyears'] = len(clean)
    i['data_min_year'] = min([i['year'] for i in clean])
    i['data_max_year'] = max([i['year'] for i in clean])
    if len(clean)>1:
        i['data_max_gap'] = max([(clean[j+1]['year']-clean[j]['year']) for j in range(len(clean)-1)])
    d['sources'].append(i)
    d['observations'][i['id']] = clean
json.dump(d,open('data/frost_complete.json','w'))
df = pd.DataFrame(d['sources'])
df.to_csv('data/frost_sources_complete.csv',index=False)


# filter sources
d = json.load(open('data/frost_complete.json','r'))
df = pd.DataFrame(d['sources'])
( (df.data_max_year >= 2018) & (df.data_nyears > 20) & (df.data_max_gap < 4) ).sum() # tune filters
d['sources'] = [i for i in d['sources'] if i['data_success']==True and i['data_max_year']>= 2018 and i['data_nyears']>20 and i['data_max_gap']<4]
d['observations'] = {k:v for k,v in d['observations'].items() if k in [i['id'] for i in d['sources']] }
json.dump(d,open('data/frost_clean.json','w'))
df = pd.DataFrame(d['sources'])
df['latitude'] = df.geometry.map(lambda i:i['coordinates'][0])
df['longitude'] = df.geometry.map(lambda i:i['coordinates'][1])
df.to_csv('data/frost_sources_clean.csv',index=False)


d = json.load(open('frost_clean.json','r'))
new = []
for i in d['sources']:
    j = {}
    for k,v in i.items():
        if k in ['id','name','shortName','masl','county','geometry'] or 'data' in k:
            j[k]=v
    del j['geometry']['nearest']
    del j['geometry']['@type']
    new.append(j)
d['sources'] = new
json.dump(d,open('frost_clean.json','w'))
