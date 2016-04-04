#!/usr/bin/env python

from Dataset import Dataset
import cPickle as pickle
from sys import argv
from time import time,gmtime,strftime
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn
import numpy as np

pklJar = open(argv[1],"rb")

prefix = 'data' if 'Data' in argv[1] else ''

pklDict = pickle.load(pklJar) # open the pickle jar
datasetSet = pklDict["datasetSet"] # eat the pickle

def saveFig(name):
  plt.savefig(prefix+name+'.png')
  plt.savefig(prefix+name+'.pdf')

def nSites(ds,utime):
  r = 0
  for k,v in ds.movement.iteritems():
    for vv in zip(v[0],v[1]):
      if vv[0] < utime < vv[1]:
        r += 1
  return r

def dictToSortedArr(d):
  l = []
  for k,v in sorted(d.iteritems()):
    l.append(v)
  return np.array(l,dtype='f')

sPerD = 60*60*24
sPerM = sPerD*30.417
nMonths=25
if prefix=='data':
  # sPerM /= 3
  nMonths = 10
now = time()-2*sPerD

# global dicts
lastCopies = {}
lastCopyVol = {}
totalVol = {}

#split by data tier
nAccesses = {}
nFiles = {}
sizeGB = {}
nReplicas = {}
nDatasets = {}

times = []
for iM in xrange(nMonths,-1,-1):
  times.append(now-iM*sPerM)
  for d in [lastCopies,lastCopyVol,totalVol]:
    d[now-iM*sPerM]=0
for d in [nAccesses,nFiles,sizeGB,nReplicas,nDatasets]:
  for tier in ['AOD','AODSIM','MINIAOD','MINIAODSIM']:
    d[tier] = {}
    for t in times:
      d[tier][t] = 0

for utime in times: 
  for ds,dobj in datasetSet.iteritems():
    n = nSites(dobj,utime)
    if n>0:   
      # it was part of the pool at this time
      totalVol[utime] += dobj.sizeGB*n
      if n==1:
        lastCopies[utime] += 1
        lastCopyVol[utime] += dobj.sizeGB
      tier = ds.split('/')[-1]
      if tier in nAccesses:
        nReplicas[tier][utime] += n
        nDatasets[tier][utime] += 1
        nFiles[tier][utime] += dobj.nFiles*n
        sizeGB[tier][utime] += dobj.sizeGB*n
        naccess = 0
        for site in dobj.nAccesses:
          for t,a in dobj.nAccesses[site].iteritems():
            if utime-sPerM < t and t <= utime:  
              # within the last month
              naccess += a
        nAccesses[tier][utime] += naccess


dates = []; lc = []

for k,v in sorted(lastCopies.iteritems()):
  dates.append(datetime.fromtimestamp(int(k)))
  lc.append(lastCopyVol[k]*1./totalVol[k])

seaborn.set_style("whitegrid")
seaborn.despine()
# seaborn.despine(left=True)

plt.plot_date(dates,lc,c=seaborn.xkcd_rgb['medium green'], ls='-', lw=2, marker='')
plt.axes().set_ylim(ymin=0,ymax=0.6)
plt.xlabel('Time',fontsize=24)
plt.ylabel('Last copy fraction',fontsize=24)
plt.gcf().set_size_inches(9.5,8)
saveFig('lastcp')

plt.clf()

for tier in ['AOD','AODSIM','MINIAOD','MINIAODSIM']:
  na = dictToSortedArr(nAccesses[tier])
  sizes = dictToSortedArr(sizeGB[tier])
  color = seaborn.xkcd_rgb['pale red'] if 'MINI' in tier else seaborn.xkcd_rgb['denim blue']
  style = '-' if 'SIM' in tier else '--'
  plt.plot_date(dates,na/sizes,c=color, ls=style, lw=2,label=tier, marker='')

plt.axes().set_ylim(ymin=0,ymax=20)
plt.xlabel('Time',fontsize=24)
plt.ylabel('Accesses per GB per month',fontsize=24)
plt.legend()
saveFig('popgb')

plt.clf()

for tier in ['AOD','AODSIM','MINIAOD','MINIAODSIM']:
  na = dictToSortedArr(nAccesses[tier])
  files = dictToSortedArr(nFiles[tier])
  color = seaborn.xkcd_rgb['pale red'] if 'MINI' in tier else seaborn.xkcd_rgb['denim blue']
  style = '-' if 'SIM' in tier else '--'
  plt.plot_date(dates,na/files,c=color, ls=style, lw=2,label=tier, marker='')

plt.axes().set_ylim(ymin=0,ymax=10)
plt.xlabel('Time',fontsize=24)
plt.ylabel('Accesses per file per month',fontsize=24)
plt.legend()
saveFig('popfile')

plt.clf()


for tier in ['AOD','AODSIM','MINIAOD','MINIAODSIM']:
  nrepl = dictToSortedArr(nReplicas[tier])
  ndata = dictToSortedArr(nDatasets[tier])
  color = seaborn.xkcd_rgb['pale red'] if 'MINI' in tier else seaborn.xkcd_rgb['denim blue']
  style = '-' if 'SIM' in tier else '--'
  plt.plot_date(dates,nrepl/ndata,c=color, ls=style, lw=2,label=tier, marker='')


plt.axes().set_ylim(ymin=0,ymax=5)
plt.xlabel('Time',fontsize=24)
plt.ylabel('Average number of replicas',fontsize=24)
plt.legend()
saveFig('replicas')

plt.clf()
