#!/usr/bin/python
#---------------------------------------------------------------------------------------------------
#
# This script uses das_client.py to extract the given dataset properties. It will determine the
# number of files and the dataset size.
#
#---------------------------------------------------------------------------------------------------
import os, sys, re, subprocess, MySQLdb

#===================================================================================================
#	H E L P E R S
#===================================================================================================
def getDbCursor():
		# configuration
		db = os.environ.get('DETOX_SITESTORAGE_DB')
		server = os.environ.get('DETOX_SITESTORAGE_SERVER')
		user = os.environ.get('DETOX_SITESTORAGE_USER')
		pw = os.environ.get('DETOX_SITESTORAGE_PW')
		# open database connection
		db = MySQLdb.connect(read_default_file = '/etc/my.cnf', read_default_group = 'mysql-ddm', db = 'IntelROCCS')
		# prepare a cursor object using cursor() method
		return db.cursor()

def getAllDatasets(columns='DatasetName',cursor=None):
	if not cursor:
		cursor = getDbCursor()
	sql = 'SELECT %s FROM Datasets'%(columns)
	cursor.execute(sql)
	return cursor.fetchall()


def getDatasetId(dataset,cursor=None):
		dbId = -1													# initialize the id (-1 -> invalid)
		# get access to the database
		if not cursor:
			cursor = getDbCursor()
		sql = "select * from Datasets where DatasetName='" + dataset + "'"
		# go ahead and try
		#print sql
		try:
				cursor.execute(sql)
				results = cursor.fetchall()
				# print results
				for row in results:
						dbId		 = row[0]
		except:
				sys.stderr.write("Could not get dataset ID %s\n"%(dataset))
				pass
		return dbId

def addDataset(dataset):
		# get access to the database
		cursor = getDbCursor()
		sql = "insert into Datasets (DatasetName) values (\'" + dataset + "\')"
		# sys.exit(-1)
		try:
				# Execute the SQL command
				print '				' + sql
				cursor.execute(sql)
		except:
				sys.stderr.write(" Error (%s): unable to insert record into table.\n"%(sql))
				return False
		return True

def addDatasetProperties(dbId,nFiles,sizeGb):
		# get access to the database
		cursor = getDbCursor()
		sql = "insert into DatasetProperties values " + \
					"(%d,%d,%f)" \
					%(dbId,nFiles,sizeGb) #added LIMIT 1 because there are duplicate entries in Datasets
		# sql = "insert into DatasetProperties values " + \
		#			 "((select DatasetId from Datasets where DatasetName='" + dataset + "' LIMIT 1),%d,%f)" \
		#			 %(nFiles,sizeGb) #added LIMIT 1 because there are duplicate entries in Datasets
		print sql
		# sys.exit(-1)
		try:
				# Execute the SQL command
				print '				' + sql
				cursor.execute(sql)
		except:
				sys.stderr.write("Error (%s): unable to insert record into table.\n"%(sql))
				return False
		return True

def checkDatabase(dataset,dbId=None,cursor=None,addMissing=True):

		nFiles = -1
		sizeGb = 0.
		# get access to the database
		if not dbId or dbId<0:
			dbId = getDatasetId(dataset,cursor)
		if dbId==-1:
			if addMissing:
				print dataset,dbId
				if (addDataset(dataset)):
						print " Added the dataset %s successfully."%(dataset)
				else:
						sys.stderr.write(" Error: Dataset %s addition failed.\n"%(dataset))
						sys.exit(1)
				dbId = getDatasetId(dataset,cursor)
			else:
				return 0,0,-1

		# make sure we have a valid id to continue
		if dbId==-1:
				sys.stderr.write("Error: unable to assign proper Id to dataset (%s). Stopping here.\n"%(dataset))
				sys.exit(3)

		# define sql
		if not cursor:
			cursor = getDbCursor()
		sql = "select * from DatasetProperties where DatasetId=" + str(dbId)
		print sql
		# go ahead and try
		try:
				cursor.execute(sql)
				results = cursor.fetchall()
				# print results
				for row in results:
						dbId		 = row[0]
						nFiles = row[1]
						sizeGb = row[2]
				#print " Properties -- Id:	%d	NFiles:	%d	Size:	%f.2"%(id,nFiles,sizeGb)
		except:
				print ' Info (%s) -- dataset properties not in database.'%(sql)
		return nFiles,sizeGb,dbId

def convertSizeToGb(sizeTxt):

		# first make sure string has proper basic format
		if len(sizeTxt) < 3:
				print ' ERROR - string for sample size (%s) not compliant. EXIT.'%(sizeTxt)
				sys.exit(1)
		# this is the text including the size units, that need to be converted
		sizeGb = float(sizeTxt[0:-2])
		units	= sizeTxt[-2:]
		# decide what to do for the given unit
		if	 units == 'MB':
				# sizeGb = sizeGb/1024.
				sizeGb = sizeGb/1000. # consistent with CMS
		elif units == 'GB':
				pass
		elif units == 'TB':
				# sizeGb = sizeGb*1024.
				sizeGb = sizeGb*1000. # consistent with CMS
		else:
				print ' ERROR - Could not identify size. EXIT!'
				sys.exit(0)

		# return the size in GB as a float
		return sizeGb

#===================================================================================================
#	M A I N
#===================================================================================================

def findDatasetProperties(dataset,short=False,cursor=None):

		debug = 0

		# first make sure not to analyze any weird data (require /*/*/* name pattern)
		if not re.search(r'/.*/.*/.*',dataset,re.S):
				sys.stderr.write(' Error: Dataset %s does NOT match expected pattern\n'%(dataset))
				sys.exit(0)

		# test whether we know this dataset already
		(nFiles,sizeGb,dbId) = checkDatabase(dataset)

		# check failed so need to go to the source
		if nFiles<0:
				# use das client to find the present size of the dataset
				cmd = os.environ.get('MONITOR_BASE') \
						+ '/das_client.py --format=plain --limit=0 --query="file dataset=' \
						+ dataset + ' | sum(file.size), count(file.name)" | sort -u'
				# if debug>-1:
				sys.stdout.write(' CMD: ' + cmd+"\n")
				nFiles = 0
				sizeGb = 0.
				try:
						for line in subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout.readlines():
								line = line[:-1]
								if	 re.search('file.name',line):
										if debug>0:
												print ' count ' + line
										nFiles = int(line.split("=")[1])
								elif re.search('file.size',line):
										if debug>0:
												print ' size	' + line
										size = line.split("=")[1]
										sizeGb = convertSizeToGb(size)
				except:
						sys.stderr.write(' Error: output data not compliant.\n')
						sys.stderr.write(' CMD: ' + cmd+"\n")
						with open(os.environ.get('MONITOR_DB')+'/datasets/blacklist.log','a') as blacklistFile:
								print "WARNING blacklisting ",dataset
								blacklistFile.write("%s\n"%(dataset))
						return -1,-1,-1 # this will never be used in readJsonSnapshotAll
						# sys.exit(0)

				# add it to our database
				addDatasetProperties(dbId,nFiles,sizeGb)

		# some derived quantities
		averageSizeGb = 0
		if nFiles>0:
				averageSizeGb = sizeGb/nFiles

		# give us the print
		# if short:
		#		 print '%d %.1f %.3f %s'%(nFiles,sizeGb,averageSizeGb,dataset)
		# else:
		#		 print ' nFiles:%d	size:%.1f GB [average file size:%.3f GB] -- dataset:%s'\
		#					 %(nFiles,sizeGb,averageSizeGb,dataset)
		return int(nFiles),sizeGb,averageSizeGb
