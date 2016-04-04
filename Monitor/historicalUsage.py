#!/usr/bin/env python

from Dataset import Dataset
import cPickle as pickle
from sys import argv
from time import time,gmtime,strftime

pklJar = open(argv[1],"rb")
pklDict = pickle.load(pklJar) # open the pickle jar
datasetSet = pklDict["datasetSet"] # eat the pickle

def nsites(ds,utime):
  r = 0
  for k,v in ds.movement.iteritems():
    for vv in zip(v[0],v[1]):
      if vv[0] < utime < vv[1]:
        r += 1
  return r

now = time()
sPerM = 60*60*24*30.417
lastCopies = {}
for iM in xrange(13):
  lastCopies[now-iM*sPerM] = 0

for ds,dobj in datasetSet.iteritems():
  for utime in lastCopies:
    if nsites(dobj,utime)==1:
      lastCopies[utime] += 1

for k,v in sorted(lastCopies.iteritems()):
  print strftime('%Y-%m',gmtime(k)),v