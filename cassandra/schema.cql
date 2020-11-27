CREATE KEYSPACE IF NOT EXISTS DataCassandraExamples WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE IF NOT EXISTS DataCassandraExamples.People (id VARCHAR PRIMARY KEY, name VARCHAR, age INT);
USE DataCassandraExamples;

INSERT INTO People (id, name, age) VALUES ('123', 'Ivanov Ivan', 23);
INSERT INTO People (id, name, age) VALUES ('321', 'Severov Arsenii', 34);