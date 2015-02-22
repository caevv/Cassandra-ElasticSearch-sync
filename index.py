from datetime import datetime
import uuid, os, sys
from cassandra.cluster import Cluster
import elasticsearch
from time import gmtime, strftime

#Cassandra: 
#CREATE TABLE user (
#  id text,
#  email text,
#  name text,
#  time timestamp,
#  PRIMARY KEY ((id))
#)
#Elastic Search
#{"users":
#{"mappings":
#{"user":
#{"properties":
#{"email":{"type":"string"},
#"id":{"type":"string"},
#"name":{"type":"string"},
#"time":{"type":"date","format":"yyyy-MM-dd HH:mm:ssZ"}
#}
#}
#}
#}
#}

#This file contains the last time it was synced
def getLastSync():
	#First time sync
	if not os.path.isfile("foo.txt"):
		insertNewSync(True)

	with open("foo.txt", "rb") as fh:
		fh.seek(-23, os.SEEK_END)
		lastsync = fh.readlines()[-1].decode()

	fh.close()
	lastsync = str.join(' ', lastsync.split()[0:23]);
	
	return lastsync


#Insert the new Sync Date
def insertNewSync(first):
	fo = open( "foo.txt", "a+" )
	#First time sync
	if first:
		fo.write("2000-01-01 10:00:00+0000"+"\n")
	else:
		fo.write(strftime("%Y-%m-%d %H:%M:%S+0000", gmtime())+"\n")

	fo.close()

	return


#This will get all date on Cassandra, It is not possible on Cassandra to get a Range of Dates
def getDataFromCassandra():
	resultCassandraAfterSync = []
	lastSyncDate = datetime.strptime(getLastSync(), '%Y-%m-%d %H:%M:%S+0000')
	
	resultCassandra = sessionCassandra.execute("SELECT * FROM user")
	
	#Now we work on its Range Date, getting just the data after the last time synced
	for i in resultCassandra:
		if i.time > lastSyncDate:
			resultCassandraAfterSync.append(i)
	
	return resultCassandraAfterSync

#Get the Data from Elastic Search, on ES it is possible to get the Range of dates
def getDataFromES():	
	esResultAfterSync = []
	doc = {
		"filter" : {
			"range" : {
				"time" : {
					"gte": getLastSync(),
					"lte": "now"
				}
			}
		}
	}
	esResultAfterSync = es.search(index='users', doc_type='user', body=doc)	

	return esResultAfterSync['hits']['hits']

	
def insertElasticSearch(id, name, email, time) :
	doc = {
		'id': id,
		'name': name,
		'email': email,
		'time': time
	}		
	es.index(index='users', id=id, doc_type='user', body=doc)
	return

def insertCassandra(id, name, email, time) :
	#Then insert to Cassandra
	query = "INSERT into user(id, name, email, time) VALUES ('%s', '%s', '%s', '%s')" % (id, name, email, time)
	sessionCassandra.execute(query);	
	return

	
#Update Based on Cassandra. When it have on Cassandra, it updates/add on ES
def updateBasedOnCassandra() :
	sizeArray = len(esResultAfterSync)

	#search for ID of Cassandra on ES - If it finds, check the index
	for i in range(len(cassandraResultAfterSync)):
	
		IdCassandra = cassandraResultAfterSync[i].id
		nameCassandra = cassandraResultAfterSync[i].name
		emailCassandra = cassandraResultAfterSync[i].email
		timeCassandra = cassandraResultAfterSync[i].time.strftime("%Y-%m-%d %H:%M:%S+0000")
		aux = 0
		#See on ES if the ID of Cassandra exists
		for x in esResultAfterSync:
			aux += 1
			
			idES = "%(id)s" %x["_source"]
			if idES == IdCassandra:
						
				#Set vars of ES
				nameES = "%(name)s" %x["_source"]
				emailES = "%(email)s" %x["_source"]
				timeES = "%(time)s" %x["_source"]
				
				#If the time of Cassandra is First (older). Update ES. ES = Cassandra
				if timeCassandra < timeES:				
					doc = {
						'doc' : {
							'name': nameCassandra,
							'email': emailCassandra,
							'time': timeCassandra
						}
					}
					es.update(index='users', doc_type='user', id=IdCassandra, body=doc)
				else:
					#update Cassandra. Cassandra = ES
					query = "UPDATE user SET name = '%s', email = '%s', time = '%s' WHERE id = '%s'" % (nameES, emailES, timeES, idES)
					sessionCassandra.execute(query);
				break
			#Don't exist on Elastic Search, so insert now
			elif aux == sizeArray:
				insertElasticSearch(IdCassandra, nameCassandra, emailCassandra, timeCassandra)

	return

def updateBasedOnES() :
	#search for ID of ES on Cassandra
	sizeArray = len(cassandraResultAfterSync)
	for i in esResultAfterSync:
		
		idES = "%(id)s" %i["_source"]
		nameES = "%(name)s" %i["_source"]
		emailES = "%(email)s" %i["_source"]
		timeES = "%(time)s" %i["_source"]
		
		aux = 0
	
		#See on Cassandra if the ID of ES exists
		for x in range(sizeArray):
			aux += 1
			
			idCassandra = cassandraResultAfterSync[x].id
			if idCassandra == idES:
				#Set vars of Cassandra
				nameCassandra = cassandraResultAfterSync[x].name
				emailCassandra = cassandraResultAfterSync[x].email
				timeCassandra = cassandraResultAfterSync[x].time.strftime("%Y-%m-%d %H:%M:%S+0000")
				
				if timeCassandra < timeES:
					#update ES. ES = Cassandra
					doc = {
						'doc' : {
							'name': nameCassandra,
							'email': emailCassandra,
							'time': timeCassandra
						}
					}		
					es.update(index='users', doc_type='user', id=idCassandra, body=doc)
				else:
					#update Cassandra. Cassandra = ES
					query = "UPDATE user SET name = '%s', email = '%s', time = '%s' WHERE id = '%s'" % (nameES, emailES, timeES, idES)
					sessionCassandra.execute(query);
				break
			#Don't exist on Elastic Search, so insert now
			elif aux == sizeArray:
				insertCassandra(idES, nameES, emailES, timeES)
	return
	
	
	
	
cluster = Cluster()
sessionCassandra = cluster.connect('users')
es = elasticsearch.Elasticsearch()

cassandraResultAfterSync = getDataFromCassandra()
esResultAfterSync = getDataFromES()
updateBasedOnCassandra()
updateBasedOnES()
insertNewSync(None)