#!/usr/bin/python
'''
Wrapper script that reads the binaries and makes the standard plots
'''

import os,sys,time

genesis=1378008000
nowish = time.time()

daysInMonth = [-1,31,28,31,30,31,30,31,31,30,31,30,31]

DDMPattern = '(?!/_/_/USER_)'
DDMGroup = 'AnalysisOps'
DDMTimeStamps = [(genesis,nowish), (time.mktime(time.strptime('2014-01-01','%Y-%m-%d')), time.mktime(time.strptime('2014-12-31','%Y-%m-%d'))) ]
DDMLabels = ["SummaryAll", "Summary2014"]
for m in range(1,13):
    firstDay = '2014-%.2i-01'%(m)
    lastDay = '2014-%.2i-%.2i'%(m,daysInMonth[m])
    DDMTimeStamps.append( (time.mktime(time.strptime(firstDay,'%Y-%m-%d')), time.mktime(time.strptime(lastDay,'%Y-%m-%d'))) )
    DDMLabels.append( "Summary2014-%.2i"%(m) )

os.environ['MONITOR_PATTERN'] = DDMPattern
os.environ['MONITOR_GROUP'] = DDMGroup
os.system('./readJsonSnapshotPickle.py T2*')

for i in range(len(DDMTimeStamps)):
    os.environ['MONITOR_PLOTTEXT'] = DDMLabels[i]
    timeStamp = DDMTimeStamps[i]
    os.system('./plotFromPickle.py T2* %i %i %s'%( timeStamp[0], timeStamp[1], '${MONITOR_DB}/monitorCache${MONITOR_GROUP}.pkl' ))


''' see our bee '''

CRBPattern = '/_/_/_AOD_'
CRBPatterns = ['/_/_/_AOD$', '/_/_/_AODSIM', '/_/_/_AOD_','/_/_/MINIAOD_']
CRBPatternLabels = ['AOD', 'AODSIM', 'AllAOD','MINIAOD']
CRBGroup = '_'
CRBTimeStamps = []
CRBLabels = ["CRBSummary12Months","CRBSummary6Months","CRBSummary3Months"]
for period in [12,6,3]:
    startTime = nowish - period*86400*30 # approximately 'period' number of months ago
    CRBTimeStamps.append((startTime,nowish))

os.environ['MONITOR_PATTERN'] = CRBPattern
os.environ['MONITOR_GROUP'] = CRBGroup
os.system('./readJsonSnapshotPickle.py T2*')

for i in range(len(CRBTimeStamps)):
    timeStamp = CRBTimeStamps[i]
    for j in range(len(CRBPatterns)):
        os.environ['MONITOR_PATTERN'] = CRBPatterns[j]
        os.environ['MONITOR_PLOTTEXT'] = CRBLabels[i]+'_'+CRBPatternLabels[j]
        os.system('./plotFromPickle.py T2* %i %i %s'%( timeStamp[0], timeStamp[1], '${MONITOR_DB}/monitorCacheAll.pkl' ))