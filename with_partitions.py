import requests, os
import checker
import sys
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

token = os.environ['my_token']
dependence = 'my_dep' 

payload = requests.get(f'api to get password/?token={token}&dependence={dependence}').json()
username, password = payload['username'], payload['password']

start_date = datetime.strptime('20210820', '%Y%m%d').date() 
start = start_date.strftime('%Y%m01') 

table_name = 'example' 
query = f"""
select *
from schema.{table_name}
where trunc(created_date, 'month') = to_date('{start}', 'yyyymmdd')
"""

url = 'jdbc:oracle:thin:@host:port/service_name|dbasename'

check = checker.Checker(uirl=url, table_name=table_name, username=username, password=password, query=query)
spark = checker.get_spark()

df = spark.read\
.format('jdbc')\
.option('url', url)\
.option('dbtable', f'({query})')\
.option('user', username)\
.option('password', password)\
.option('driver', 'oracle.jdbc.driver.OracleDriver')\
.load()

df.write.mode('overwrite').parquet(f'hdfs_path/{table_name}/p_month={start}')
spark.sql(f'MSCK REPAIR TABLE HIVE_SCHEMA.{table_name}')
check.check_and_exit()
