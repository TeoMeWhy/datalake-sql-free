-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS bronze_olist LOCATION '/mnt/datalake/bronze/olist';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver_olist LOCATION '/mnt/datalake/silver/olist';

GRANT ALL PRIVILEGES ON SCHEMA silver_olist TO professores;

GRANT USAGE ON SCHEMA silver_olist TO alunos;
GRANT SELECT ON SCHEMA silver_olist TO alunos;
GRANT READ_METADATA ON SCHEMA silver_olist TO alunos;


