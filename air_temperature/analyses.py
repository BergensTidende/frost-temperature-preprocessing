import os, json, requests
import pandas as pd
import geopandas
from plotnine import *

os.chdir('/Users/adroesch/Documents/git/20-01_frost-data/air_temperature/')

# plot all stations 10y avgs
d = json.load(open('frost_clean.json','r'))
s = d['sources']
# [i for i in s if 'bergen' in i['municipality'].lower()]
o = d['observations']
for k,v in o.items():
    for i in v:
        i['sourceId'] = k
o = pd.DataFrame([j for i in o.values() for j in i])
o = o.sort_values(['sourceId','year'],ascending=True)
o['rollavg_10y'] = o.groupby('sourceId')['value'].rolling(10).mean().values

ggplot(o.loc[~o.rollavg_10y.isnull()],aes(x='year',y='rollavg_10y',color='sourceId'))+geom_line()+scale_color_discrete(guide=False)

# plot 10y avg temperature increases by decades
decs = []
for i in range(1940,2005,5):
    x = o.loc[o.year.isin([i,2019]),['sourceId','year','rollavg_10y']]
    x = x.pivot_table(index='sourceId',columns='year').dropna()
    x['rollavg_10y_temp_increase'] = x['rollavg_10y'].diff(axis=1)[2019]
    x = x.drop(columns=['rollavg_10y'])
    x['period'] = str(i)+'-2019'
    x['period_start_year'] = str(i)
    decs.append(x)
decs = pd.concat(decs)
ggplot(decs,aes(x='period_start_year',y='rollavg_10y_temp_increase'))+geom_point()
ggplot(decs,aes(x='period_start_year',y='rollavg_10y_temp_increase'))+geom_violin()

# get global indicators and histogram of all periods temperature  increases
decs.rollavg_10y_temp_increase.mean()
decs.rollavg_10y_temp_increase.median()
ggplot(decs,aes('rollavg_10y_temp_increase'))+geom_histogram()

# comparison with linear regression
from scipy.stats import linregress
comps = []
for i in range(1940,2005,5):
    for j in o.sourceId.unique():
        x = o.loc[(o.sourceId==j)].dropna()
        if not i in x.year.values or not 2019 in x.year.values:
            continue
        lin_mod = linregress(x.year,x.value)
        comps.append({
            'period' : str(i)+'-2019',
            'period_start_year' : i,
            'temp_increase': x.rollavg_10y[x.year==2019].values[0] - x.rollavg_10y[x.year==i].values[0],
            'lin_slope': lin_mod.slope,
            'lin_increase' : lin_mod.slope*(2019-i),
            'lin_pvalue' : lin_mod.pvalue
        })
c = pd.DataFrame(comps)
ggplot(c,aes(x='lin_increase',y='temp_increase',color='period_start_year'))+geom_point()

c['estimate_diff'] = c.temp_increase - c.lin_increase
c.estimate_diff.mean()
c.estimate_diff.median()
(c.estimate_diff>0).mean()
ggplot(c,aes('estimate_diff'))+geom_histogram()

ggplot(c,aes(x='period_start_year',y='estimate_diff'))+geom_point()+geom_smooth(method='lm')
