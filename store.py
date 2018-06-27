import requests
import json
import psycopg2
import sys
import re
from urllib.parse import quote 
#import progressbar
#import time
#We are going to preprocess the data before we start editing it!
if len(sys.argv) == 1:
	print("Please enter in a filename followed by a tablename")
	sys.exit()
conn_string = "host='localhost' dbname='zillow_2017_Nov_ZTrans' user='postgres' password='bdeep'"
print ("Connecting to database\n host = localhost dbname='zillow_2017_Nov_ZTrans' user ='postgres'\n")
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print("FILTERING/INSERTING DATA")
filename = '/'+sys.argv[1]
DB_num = '/06'
DB_name = '/ZTrans'
ogFile = open('/mnt/share/projects/Zillow_Housing/stores/Zillow_2017_Nov'+ DB_num + DB_name + filename, 'r')
errFile = open('failedAttempts.txt','w+')
completionFile = open('Data_Stored.txt', 'a+')
tableName = sys.argv[2]
print("Table = " +tableName+ " for State Num: "+ DB_num)
progress = 0
tracker = 0
with ogFile as fileObj:
	try:
		for line in fileObj:
			progress += 1
			listFile = []
			line = line.rstrip('\n')
			line.encode('utf-8').strip()
			line = line.replace("'","")
			listFile= line.split('|')
			counter = 0
			values = ''
			for item in listFile:
				if counter < len(listFile):
					if item == "" or item == '\x00':
						listFile[counter] = 'null'
					else:
						listFile[counter] = "'"+listFile[counter]+"'"
					counter = counter + 1
			for item in listFile:
				if values == "":
					values = values + item
				else:
					values = values + ',' + item
			try:
				cursor.execute("SAVEPOINT recovery")
				cursor.execute(""" INSERT INTO %s VALUES (%s)"""%((tableName),values))
			except psycopg2.DataError as err:
				errFile.write("FAILED ATTEMPT, TRY AGAIN: ")
				errFile.write("INSERT INTO %s VALUES (%s)\n"%((tableName), values))
				errFile.write("ERROR WAS %s \n\n"%(err))
				progress -= 1
				cursor.execute("ROLLBACK TO SAVEPOINT recovery")
			counter = 0
			tracker += 1
			cursor.execute("RELEASE recovery")
			if progress % 100000 == 0 and progress is not 0:
				conn.commit()
				print("Commited %s entries"%(progress))
	except UnicodeDecodeError as err:
		line.replace("0xa5", " ")
		
print("Final Commit of %s entries"%(progress))
print("Processed %s lines"%(tracker))		
conn.commit()
if tracker is not 0:
	final_percent = (progress/tracker)*100
else:
	final_percent = 100
completionFile.write("Finished storing "+filename[1:]+" from State Code: "+DB_num[1:]+" in zillow_2017_Nov_"+DB_name[1:]+" with " + str(final_percent)+"%  of the data inserted successfully\n")
completionFile.close()
ogFile.close()
errFile.close()
