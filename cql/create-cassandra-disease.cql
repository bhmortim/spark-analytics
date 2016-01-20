-- DROP KEYSPACE IF EXISTS disease;
CREATE KEYSPACE IF NOT EXISTS disease WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
DESCRIBE KEYSPACES;

USE disease;

-- ----------------------------------- --
CREATE TABLE IF NOT EXISTS 
hepatitis (
    url text PRIMARY KEY
);
-- ----------------------------------- --

DESCRIBE KEYSPACE disease;
