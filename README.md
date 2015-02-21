# Cassandra-ElasticSearch-sync
Sync Between Cassandra and ElasticSearch using Python

This is a Sync between a Elastic Search and Cassandra. 

# How to

Just python index.py on console

#How it works

It gets all data of Cassandra and then **for** getting just the date after the last Sync. On Elastic Search, the range date is already sorted.

After then, it will check again the rows of each one and look if the data exists in the other, if no, it will add to the one who don't have. If already exists, it will check the first one (the older), and then update the other with its datas.
