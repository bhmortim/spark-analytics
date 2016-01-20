-- DROP KEYSPACE IF EXISTS disease;
CREATE KEYSPACE IF NOT EXISTS disease WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;

USE disease;

-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS health    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS cancer    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS hepatitis ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS brain     ( url text PRIMARY KEY );
-- ---------------------------------------------------------- --

DESCRIBE KEYSPACE disease;
