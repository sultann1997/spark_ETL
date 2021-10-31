import requests, os
import checker

token = os.environ['my_token']
dependence = 'my_dep'

payload = requests.get(f'api to get passwords/?token={token}&dependence={dependence}').json()['payload']
username, password = payload['userName'], payload['passwd']

table_name = 'example'
query = f"""
select * from schema.{table_name}
"""
url = 'jdbc:oracle:thin:@host:port/service_name|dbasename'

check = checker.Checker(url, table_name, username, password, query)
spark = checker.get_spark()

df = spark.read\
.format('jdbc')\
.option('url', url)\
.option('dbtable', f'({query})')\
.option('user', username)\
.option('password', password)\
.option('driver', 'oracle.jdbc.driver.OracleDriver')\
.load()

df.write.mode('overwrite').parquet(f'hdfs_path/{table_name}')

check.check_and_exit()
