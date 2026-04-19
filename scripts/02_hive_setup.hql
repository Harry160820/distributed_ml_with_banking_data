-- Creates Hive database and external tables pointing to HDFS.
-- Run with: hive -f scripts/02_hive_setup.hql

SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=true;
SET hive.fetch.task.conversion=more;
SET hive.exec.parallel=false;

CREATE DATABASE IF NOT EXISTS banking_db;
USE banking_db;

DROP TABLE IF EXISTS bank_customers;

CREATE EXTERNAL TABLE IF NOT EXISTS bank_customers (
    age INT,
    job STRING,
    marital STRING,
    education STRING,
    defaulted STRING,
    balance INT,
    housing STRING,
    loan STRING,
    contact STRING,
    day INT,
    month STRING,
    duration INT,
    campaign INT,
    pdays INT,
    previous INT,
    poutcome STRING,
    y STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/banking/raw/bank'
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW TABLES;

SELECT * 
FROM bank_customers
LIMIT 5;