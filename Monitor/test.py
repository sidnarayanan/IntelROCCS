#!/usr/bin/env python


import dynamoDB
import time

cursor = dynamoDB.getDbCursor()

sql = 'SELECT d.`name`, s.`name`, da.`date`, da.`num_accesses` FROM `dataset_accesses` AS da     INNER JOIN `datasets` AS d ON d.`id` = da.`dataset_id`   INNER JOIN `sites` AS s ON s.`id` = da.`site_id`  ORDER BY da.`date` ASC;' 
cursor.execute(sql)

for r in cursor.fetchall():
    if r [0] == '/Charmonium/Run2017B-22Jun2017-v1/MINIAOD':
        print r
