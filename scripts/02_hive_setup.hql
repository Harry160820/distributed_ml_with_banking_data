-- Creates Hive database and external tables pointing to HDFS.
-- Run with: hive -f scripts/02_hive_setup.hql

CREATE DATABASE IF NOT EXISTS banking_db;

USE banking_db;

-- External table: reads bank.csv in-place from HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS bank_customers (

    age       INT,
    job       STRING,
    marital   STRING,
    education STRING,
    defaulted STRING,
    balance   INT,
    housing   STRING,
    loan      STRING,
    contact   STRING,
    day       INT,
    month     STRING,
    duration  INT,
    campaign  INT,
    pdays     INT,
    previous  INT,
    poutcome  STRING,
    subscribed STRING
)

ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ';'

STORED AS TEXTFILE
LOCATION '/banking/raw/bank/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Verify
SELECT COUNT(*) AS total_rows FROM bank_customers;
SELECT subscribed, COUNT(*) AS cnt
FROM bank_customers
GROUP BY subscribed;

-- Analytical queries for the report
SELECT job, AVG(balance) AS avg_balance, COUNT(*) AS cnt
FROM bank_customers
GROUP BY job
ORDER BY avg_balance DESC;
