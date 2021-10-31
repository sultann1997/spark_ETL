import findspark
findspark.init('/mnt/nfs/spark')
findspark.find()
 
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window, HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count
from datetime import datetime, timedelta
import requests
import subprocess
import sys
import decimal
import os

os.environ['SPARK_CONF_DIR'] = 'custom_conf'

conf = pyspark.SparkConf().setAll([
    ('spark.ui.port', '3778'),
    ('spark.cores.max', '1'),
    ('spark.executor.memory', '512m'),
    ('spark.driver.extraClassPath', 'ojdbc7.jar:postgresql-42.2.20.jre7.jar'),
    ('spark.executor.extraClassPath', 'ojdbc7.jar:postgresql-42.2.20.jre7.jar'),
    ('spark.sql.parquet.writeLegacyFormat', 'True'),
    ('spark.sql.parquet.compression.codec','snappy'),
    ('spark.sql.catalogImplementation', 'hive'),
    ('spark.sql.sources.partitionOverwriteMode', 'dynamic')])
 
master = 'mesos://zk://dserver02:2181/mesos' #mesos master
 
conf.setMaster(master)
spark = pyspark.sql.SparkSession\
.builder\
.master(master)\
.appName('creating hive table')\
.config(conf=conf)\
.enableHiveSupport()\
.getOrCreate()
 
spark

def create_hive_table(dependence, schema, table_name, location, partitioned=None, hdfs_table_name = None):
    if partitioned == None:
        partitioned = ''
    else:
        partitioned = f'PARTITIONED BY ({partitioned} string)'
    if hdfs_table_name == None:
        hdfs_table_name = f"{schema}.{table_name}"
    else:
        hdfs_table_name = hdfs_table_name
    token = os.environ['my_token']
    payload = requests.get(f'api with passwords/?token={token}&dependence={dependence}').json()
    username, password = payload['username'], payload['password']
    
    dep_dict = {
        'dep1' : {'url' : 'jdbc:oracle:thin:@host1:port1/dbase1', 'driver' : 'oracle.jdbc.driver.OracleDriver'},
        'dep2' : {'url' : 'jdbc:oracle:thin:@host2:port2/dbase2', 'driver' : 'oracle.jdbc.driver.OracleDriver'},
        'dep3' : {'url' : 'jdbc:postgresql://postgre-host1:postgre-port1/postgre-dbase1', 'driver' : 'org.postgresql.Driver'},
        'dep4' : {'url' : 'jdbc:postgresql://postgre-host2:postgre-port2/postgre-dbase2', 'driver' : 'org.postgresql.Driver'}
    }
    url = dep_dict[dependence.lower()]['url']
    driver = dep_dict[dependence.lower()]['driver']
    print(url)
    if driver == 'oracle.jdbc.driver.OracleDriver':
        query_cols = f"""
        SELECT COLUMN_NAME, COMMENTS FROM all_col_comments
        WHERE OWNER='{schema.upper()}' and TABLE_NAME = '{table_name.upper()}'
        """
    elif driver == 'org.postgresql.Driver':
        query_cols = f""" SELECT t.attname AS COLUMN_NAME, t.col_description as COMMENTS from (
        SELECT a.attname,
        pg_catalog.format_type(a.atttypid, a.atttypmod),
        (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
        FROM pg_catalog.pg_attrdef d
        WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
        a.attnotnull, a.attnum,
        (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
        WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
        a.attidentity,
        NULL AS indexdef,
        NULL AS attfdwoptions,
        a.attstorage,
        CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = (SELECT c.oid
        FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname OPERATOR(pg_catalog.~) '^({table_name.lower()})$'
        AND n.nspname OPERATOR(pg_catalog.~) '^({schema.lower()})$')
        AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum) t
        """
    print(query_cols)
    cols_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", f"{query_cols}") \
        .option("user", f"{username}") \
        .option("password", f"{password}") \
        .option("driver", driver) \
        .load()
    cols_df = cols_df.select([col(x).alias(x.upper()) for x in cols_df.columns])
    empty_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", f"select * from {schema}.{table_name} where 1=0") \
        .option("user", f"{username}") \
        .option("password", f"{password}") \
        .option("driver", driver) \
        .load()

    so = ''
    cols_list = cols_df.collect()
    print(cols_list)
    for i in empty_df.dtypes:
        comment = 'None'
        for j in cols_list:
            if i[0].upper() == j.COLUMN_NAME.upper():
                comment = j.COMMENTS
                cols_list.remove(j)
                break
        s = f"`{i[0]}` {i[1]} COMMENT '{comment}',"
        so += s
    so = so[:-1]
    create_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {hdfs_table_name}({so})
    STORED AS PARQUET
    {partitioned}
    LOCATION '{location}';
    """
    print(create_query)
    spark.sql(create_query)
    
if __name__ == '__main__':
    dependence = sys.argv[1]
    schema = sys.argv[2]
    table = sys.argv[3]
    location = sys.argv[4]
    try:
        partitioned = sys.argv[5]
        if partitioned == "":
            partitioned = None
    except:
        partitioned = None
    try:
        hdfs_table_name = sys.argv[6]
    except:
        hdfs_table_name = None
    try:
        create_hive_table(dependence, schema, table, location, partitioned, hdfs_table_name)
    except Exception as e:
        print(e)
    spark.stop()
