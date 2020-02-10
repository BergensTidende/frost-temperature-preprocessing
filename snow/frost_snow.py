import dateutil.parser
from datetime import datetime
import os, json, requests
from requests.auth import HTTPBasicAuth
import pandas as pd

# os.chdir('/Users/adroesch/Documents/git/20-01_frost-data')

# get all available timeseries
a = requests.get('https://frost.met.no/observations/availableTimeSeries/v0.jsonld',
      params={
        'elements':'surface_snow_thickness'
      },
      auth=HTTPBasicAuth(os.environ['FROST_KEY'],os.environ['FROST_SECRET'])
)
sources = a.json()['data']
sources = [i for i in sources if i['sourceId'].split(':')[0][:2]=='SN'] #filter sensory system

# get observations
ids = list(set([i['sourceId'].split(':')[0] for i in sources]))
for i in ids:
    r = requests.get('https://frost.met.no/observations/v0.jsonld',
        params={
            'elements' : 'surface_snow_thickness',
            'sources': i,
            'referencetime':'1900-01-01T00:00:00Z/2020-01-01T00:00:00Z'
        },
        auth=HTTPBasicAuth(os.environ['FROST_KEY'],os.environ['FROST_SECRET'])
    )
    if r.status_code == 200:
        json.dump(r.json()['data'],open('data/observations_snow/'+i+'.json','w'))
    elif r.status_code==403:
        found = False
        for j in range(1900,2020,10):
            r = requests.get('https://frost.met.no/observations/v0.jsonld',
                params={
                    'elements' : 'surface_snow_thickness',
                    'sources': i,
                    'referencetime':f'{j}-01-01T00:00:00Z/{j+10}-01-01T00:00:00Z'
                },
                auth=HTTPBasicAuth(os.environ['FROST_KEY'],os.environ['FROST_SECRET'])
            )
            if r.status_code == 200:
                json.dump(r.json()['data'],open(f'data/observations_snow/{i}_{j}.json','w'))
                found=True
        if not found:
            with open('data/failed_snow_sources.txt','a') as f:
                f.write(i+'\n')
    else:
        with open('data/failed_snow_sources.txt','a') as f:
            f.write(i+'\n')

# get sources meta
files = os.listdir('data/observations_snow/')
ids = list(set([i.split('.')[0].split('_')[0] for i in files]))
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
            'ids': idCsv
          },
          auth = HTTPBasicAuth( os.environ['FROST_KEY'], os.environ['FROST_SECRET'])
    )
    if r.status_code == 200:
        metas = metas + r.json()['data']
    else:
        print(i)
    i=i+1
len(metas)
json.dump(metas,open('data/files_snow_metas.json','w'))


# clean data
metas = json.load(open('data/files_snow_metas.json','r'))
d = {
    'sources': [],
    'observations': {}
}

fail_2019 = 0
fail_gap = 0
for i in metas:
    try:
        source_files = [j for j in os.listdir('data/observations_snow') if j.split('.')[0].split('_')[0]==i['id']]
        if len(source_files)>0:
            obs = []
            for j in source_files:
                obs = obs + json.load(open('data/observations_snow/'+j,'r'))
        else:
            obs = json.load(open('data/observations_snow/'+i['id']+'.json','r'))
            j = obs[2]
        daily_obs = [j for j in obs if j['referenceTime'][11:]=='00:00:00.000Z']
        i['data_success'] = True
    except:
        i['data_success'] = False
        continue
    clean = [{
        'date': j['referenceTime'][:10],
        'value': j['observations'][0]['value']
    } for j in obs]
    clean.sort(key=lambda i:i['date'])
    clean_years = list(set(int(j['date'][:4]) for j in clean))
    i['data_min_year'] = min(clean_years)
    i['data_max_year'] = max(clean_years)
    i['data_nyears'] = i['data_max_year']-i['data_min_year']
    if len(clean_years)>1:
        date_diffs = [
        (datetime.strptime(clean[j+1]['date'],'%Y-%m-%d') - datetime.strptime(clean[j]['date'],'%Y-%m-%d')).days
            for j in range(len(clean)-1)]
        i['data_max_gap'] = max(date_diffs)
    d['sources'].append(i)
    d['observations'][i['id']] = clean
json.dump(d,open('data/frost_snow_complete.json','w'))
df = pd.DataFrame(d['sources'])
df.to_csv('data/frost_sources_snow_complete.csv',index=False)


# filter sources
d = json.load(open('data/frost_snow_complete.json','r'))
df = pd.DataFrame(d['sources'])
( (df.data_max_year >= 2018) & (df.data_nyears > 20) & (df.data_max_gap < 4) ).sum() # tune filters
d['sources'] = [i for i in d['sources'] if i['data_success']==True and i['data_max_year']>= 2018 and i['data_nyears']>20 and i['data_max_gap']<4]
d['observations'] = {k:v for k,v in d['observations'].items() if k in [i['id'] for i in d['sources']] }
json.dump(d,open('data/frost_clean.json','w'))
df = pd.DataFrame(d['sources'])
df['latitude'] = df.geometry.map(lambda i:i['coordinates'][0])
df['longitude'] = df.geometry.map(lambda i:i['coordinates'][1])
df.to_csv('data/frost_sources_clean.csv',index=False)
