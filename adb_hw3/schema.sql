-- Schema definition for ADB Homework 2

-- Primary table used by Blocking.java, RestartBlocking.java, and NoLiveLocks.java
DROP TABLE IF EXISTS blocking_data;
CREATE TABLE blocking_data (
    data_id integer PRIMARY KEY,
    data_value float,
    modified_by integer
);

-- Additional table_adb_hw2 if you wish to use or test with this name
DROP TABLE IF EXISTS table_adb_hw2;
CREATE TABLE table_adb_hw2 (
    data_id integer PRIMARY KEY,
    data_value float,
    modified_by integer
);

