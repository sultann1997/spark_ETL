import checker
import os, requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

token = os.environ['my_token']
dependence = 'my_dep'
 
payload = requests.get(f'api to get password/?token={token}&dependence={dependence}').json()
username, password = payload['username'], payload['password']

start_date = datetime.strptime('20210301', '%Y%m%d').date() 
end_date = datetime.now().date()
start = start_date.strftime('%Y%m01')

spark = checker.get_spark()

while start_date <= end_date:
    table_name = 'example' 
    query = f"""
    select *
                    from schema.{table_name}
                  where trunc(created_date, 'month') = to_date('{start}', 'yyyymmdd')
    """
    url = 'jdbc:oracle:thin:@host:port/dbase'
    df = spark.read\
        .format('jdbc')\
        .option('url', url)\
        .option('dbtable', f'({query})')\
        .option('user', username)\
        .option('password', password)\
        .option('driver', 'oracle.jdbc.driver.OracleDriver')\
        .load()
   
    df.write.mode('overwrite').parquet(f'hdfs_path/{table_name}/p_month={start}') 
    spark.sql(f'MSCK REPAIR TABLE HIVE_SCHEMA.{table_name}')

    checker.Checker(url, table_name, username, password, query).check_init()

    start_date = start_date + relativedelta(months=1) 
    start = start_date.strftime('%Y%m%d')

spark.stop()
sys.exit(0)
