-- DROP KEYSPACE IF EXISTS disease;
CREATE KEYSPACE IF NOT EXISTS disease WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
DESCRIBE KEYSPACES;

USE disease;

-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS health    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS cancer    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS hepatitis ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS brain     ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS biology   ( url text PRIMARY KEY );
-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS mental    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS headache  ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS cardio    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS disease   ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS heart     ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS fungal    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS gerson    ( url text PRIMARY KEY );
CREATE TABLE IF NOT EXISTS vulgaris  ( url text PRIMARY KEY );
-- ---------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS general  ( condition text,
                                      url text,
                                      PRIMARY KEY (condition, url)
                                     );
-- ---------------------------------------------------------- --

DESCRIBE KEYSPACE disease;
