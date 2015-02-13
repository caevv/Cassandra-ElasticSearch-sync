# Cassandra-ElasticSearch-sync
Sync Between Cassandra and ElasticSearch using Python


Cassandra Keyspace test; Table user:
create table user (
        ... user_id varchar primary key,
        ... first varchar,
        ... last varchar,
        ... age int
        ... );

Initial Data:
insert into user (user_id, first, last, age) values ('cvecchi', 'vecchi', 'francois', 30);
