import findspark
findspark.init('/mnt/nfs/spark')
findspark.find()
    
import pyspark
from pyspark.sql.functions import col
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys, os, re

triggered = False

def get_spark(app_name='Loading_table', dbase = 'all', memory='512m', *, submit_args={}):
    os.environ['SPARK_CONF_DIR'] = 'custom_conf'
    
    if len(submit_args) > 0:
        conf_str = ""
        for config in submit_args:
            conf_str += f"--{config} {submit_args[config]}"
        conf_str += " pyspark-shell"
        os.environ['PYSPARK_SUBMIT_ARGS'] = conf_str

    if dbase.lower() == 'oracle':
        conf = pyspark.SparkConf().setAll([
            ('spark.ui.port', '3778'),
            ('spark.cores.max', '2'),
            ('spark.executor.memory', memory),
            ('spark.driver.memory', '512m'),
            ('spark.driver.extraClassPath', 'jdbc-connectors/ojdbc7.jar'),
            ('spark.executor.extraClassPath', 'jdbc-connectors/ojdbc7.jar'),
            ('spark.sql.parquet.writeLegacyFormat', 'True'),
            ('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'LEGACY'),
            ('spark.sql.parquet.compression.codec','snappy'),
            ('spark.sql.catalogImplementation', 'hive'),
            ('spark.sql.sources.partitionOverwriteMode', 'dynamic'),
            ('spark.dynamicAllocation.enabled', 'false')])
    elif dbase.lower() == 'postgre':
        conf = pyspark.SparkConf().setAll([
            ('spark.ui.port', '3778'),
            ('spark.cores.max', '2'),
            ('spark.executor.memory', memory),
            ('spark.driver.memory', '512m'),
            ('spark.driver.extraClassPath', 'postgresql-42.2.20.jre7.jar'),
            ('spark.executor.extraClassPath', 'postgresql-42.2.20.jre7.jar'),
            ('spark.sql.parquet.writeLegacyFormat', 'True'),
            ('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'LEGACY'),
            ('spark.sql.parquet.compression.codec','snappy'),
            ('spark.sql.catalogImplementation', 'hive'),
            ('spark.sql.sources.partitionOverwriteMode', 'dynamic'),
            ('spark.dynamicAllocation.enabled', 'false')])
    elif dbase.lower() == 'all':
         conf = pyspark.SparkConf().setAll([
            ('spark.ui.port', '3778'),
            ('spark.cores.max', '2'),
            ('spark.executor.memory', memory),
            ('spark.driver.memory', '512m'),
            ('spark.driver.extraClassPath', 'postgresql-42.2.20.jre7.jar:\
            ojdbc7.jar'),
            ('spark.executor.extraClassPath', 'postgresql-42.2.20.jre7.jar:\
            ojdbc7.jar'),
            ('spark.sql.parquet.writeLegacyFormat', 'True'),
            ('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'LEGACY'),
            ('spark.sql.parquet.compression.codec','snappy'),
            ('spark.sql.catalogImplementation', 'hive'),
            ('spark.sql.sources.partitionOverwriteMode', 'dynamic'),
            ('spark.dynamicAllocation.enabled', 'false')])
            
    master = 'mesos://zk://mesosserver:mesosport/mesos' #mesos master
    conf.setMaster(master)
    global spark
    spark = pyspark.sql.SparkSession\
    .builder\
    .master(master)\
    .appName(app_name)\
    .config(conf=conf)\
    .enableHiveSupport()\
    .getOrCreate()
    spark
    return spark

class Checker:
    """Module for checking data integrity after export from Oracle to Hadoop"""
    def __init__(self, url, table_name, username, password, query, hdfs_name = None, *, schema=None, part_dict=None, hdfs_query = None):
        self.connection_link = url
        self.schema_name = schema
        self.table_name = table_name
        self.username = username
        self.password = password
        if len(query.split()) > 1:
            self.ora_query = query
        else:
            self.ora_query = f"select * from {query.split()[0]}"
            
        global triggered
        
        if query.upper().find(f'PARTITION') != -1:
            part_pattern = "\((.*?)\)"
            part_query = query.upper()[query.upper().find(f'PARTITION'):]
            self.ora_partition = re.search(part_pattern, part_query).group(1)
            if triggered == False:
                self.part_dict = self.get_part_date()
                self.rpart_dict = dict((value,key) for key, value in self.part_dict.items())
                triggered = True
        else:
            self.ora_partition = None
            self.part_dict = None
            self.rpart_dict = None
        if part_dict != None:
            self.part_dict = part_dict
            self.rpart_dict = dict((value,key) for key, value in self.part_dict.items())
        
        if schema != None:
            self.schema = schema
        else:
            self.schema = 'DWH'
            
        if hdfs_name != None:
            self.hdfs_table_name = hdfs_name
        else:
            self.hdfs_table_name = f"{self.schema}.{table_name}"
        
        self.numeric_columns = [
            'numeric', 'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'serial', 'bigserial'
        ]
        
        self.hdfs_query = hdfs_query
        
    def get_part_date(self):
        part_query = f"select PARTITION_NAME, HIGH_VALUE from all_tab_partitions where table_name = '{self.table_name.upper()}'"
        df_part = spark.read\
            .format('jdbc')\
            .option('url', self.connection_link)\
            .option('dbtable', f"({part_query})")\
            .option('user', self.username)\
            .option('password', self.password)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver')\
            .load()
        partitions_list = df_part.collect()
        partitions_dict = {}
        for partition in partitions_list:
            pattern = "TO_DATE\('(.*?)',"
            date = re.search(pattern, partition.HIGH_VALUE).group(1).strip().split(' ')[0].replace('-','')
            date = datetime.strftime(datetime.strptime(date, '%Y%m%d') - relativedelta(days=1), '%Y%m%d')
            partitions_dict[date] = partition.PARTITION_NAME
        print('Made a partitions dict')
        return partitions_dict
    
    def get_ora_shape(self):
        """
        Getting the shape of the table in oracle, as well as getting the checks
        """
        query_cols = f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, HIDDEN_COLUMN FROM ALL_TAB_COLS
        WHERE OWNER = '{self.schema.upper()}' AND TABLE_NAME = '{self.table_name.upper()}'
        """
        self.cols_df = spark.read \
            .format("jdbc") \
            .option("url", self.connection_link) \
            .option("dbtable", f"({query_cols})") \
            .option("user", f"{self.username}") \
            .option("password", f"{self.password}") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        query_count = ""
        query_sums = ""
        self.cols_df = self.cols_df.filter(self.cols_df.HIDDEN_COLUMN != 'YES')
        for i in self.cols_df.collect():
            if i[1] != 'CLOB':
                query_count += f"""count("{i[0]}") {i[0]}_C, """
                if i[1].startswith('NUMBER'):
                    query_sums += f"""sum("{i[0]}") {i[0]}_S, """
        if len(query_sums) > 0:
            query_ = query_count + query_sums[:-2]
        else:
            query_ = query_count[:-2]
        self.query_final = f"""
        SELECT COUNT(*) as TOTAL_COUNT, {query_} FROM ({self.ora_query})
        """
        if self.hdfs_query == None:
            self.query_final_hdfs = self.query_final
        else:
            self.query_final_hdfs = f"""
            SELECT COUNT(*) as TOTAL_COUNT, {query_} FROM ({self.hdfs_query})
            """
        self.ora_checks = spark.read \
            .format("jdbc") \
            .option("url", self.connection_link) \
            .option("dbtable", f"({self.query_final})") \
            .option("user", f"{self.username}") \
            .option("password", f"{self.password}") \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load().collect()[0]
        rows = int(self.ora_checks['TOTAL_COUNT'])
        cols = int(self.cols_df.select('COLUMN_NAME').count())
        self.ora_shape = (rows, cols)
    
    def get_postgre_shape(self):
        """
        Getting the shape of the table in postgre, as well as getting the checks
        """
        query_cols = f"""
        SELECT column_name, data_type from information_schema.columns
        where table_schema = '{self.schema.lower()}' and table_name = '{self.table_name.lower()}'
        """
        temp_df = spark.read \
            .format("jdbc") \
            .option("url", self.connection_link) \
            .option("dbtable", f"({query_cols}) a") \
            .option("user", f"{self.username}") \
            .option("password", f"{self.password}") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        self.cols_df = temp_df.select([col(x).alias(x.upper()) for x in temp_df.columns])
        query_count = ""
        query_sums = ""
        for i in self.cols_df.collect():
            query_count += f"count(a.{i[0]}) {i[0]}_C, "
            if i[1] in self.numeric_columns:
                query_sums += f"sum(a.{i[0]}) {i[0]}_S, "
        if len(query_sums) > 0:
            query_ = query_count + query_sums[:-2]
        else:
            query_ = query_count[:-2]
        self.query_final = f"""
        SELECT COUNT(*) as TOTAL_COUNT, {query_} FROM ({self.ora_query}) a
        """
        if self.hdfs_query == None:
            self.query_final_hdfs = self.query_final
        else:
            self.query_final_hdfs = f"""
            SELECT COUNT(*) as TOTAL_COUNT, {query_} FROM ({self.hdfs_query})
            """
        temp_df = spark.read \
            .format("jdbc") \
            .option("url", self.connection_link) \
            .option("dbtable", f"({self.query_final}) t") \
            .option("user", f"{self.username}") \
            .option("password", f"{self.password}") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        temp_df = temp_df.select([col(x).alias(x.upper()) for x in temp_df.columns])
        self.ora_checks = temp_df.collect()[0]
        rows = int(self.ora_checks['TOTAL_COUNT'])
        cols = int(self.cols_df.select('COLUMN_NAME').count())
        self.ora_shape = (rows, cols)
        
    def get_hive_shape(self):
        spark.sql(f'REFRESH TABLE {self.hdfs_table_name};')
        try:
            spark.sql(f'MSCK REPAIR TABLE {self.hdfs_table_name}')
        except:
            pass
        hive_cols = spark.sql(f'show columns from {self.hdfs_table_name}').collect()
        try:
            partition = [column.col_name for column in hive_cols
                         if column.col_name.lower() in ['p_day', 'p_month', 'p_year']][0]
        except:
            partition = None
        hive_cols = [column.col_name for column in hive_cols
                    if column.col_name.lower() not in ['p_day', 'p_month', 'p_year', 'report']]
        hive_query = self.query_final_hdfs.upper().replace(f"{self.schema.upper()}.{self.table_name.upper()}", self.hdfs_table_name.upper())
        if partition != None:
            if self.ora_partition == None and self.part_dict == None:
                pattern = "TO_DATE\('(.*?)'"
                date_search = re.search(pattern, hive_query)
                if date_search != None:
                    date = date_search.group(1)
                else:
                    date = None
                hdfs_check_query = hive_query[:hive_query.find('WHERE')] + f"WHERE {partition} = '{date}') a"
            else:
                date = self.rpart_dict[self.ora_partition]
                hdfs_check_query = hive_query[:hive_query.find(f'PARTITION')] + f"WHERE {partition} = '{date}') a"
        else:
            date = None
            hdfs_check_query = hive_query
        if date == None:
            hdfs_check_query = hive_query
        try:
            temp_df = spark.sql(hdfs_check_query.replace('"', '`'))
        except Exception as e:
            print(hdfs_check_query)
            print('WITH REPLACEMENT\n', hdfs_check_query.replace('"', '`'))
            raise(e)
        temp_df = temp_df.select([col(x).alias(x.upper()) for x in temp_df.columns])
        self.hdfs_checks = temp_df.collect()[0]
        cols = len(hive_cols)
        rows = self.hdfs_checks['TOTAL_COUNT']
        self.hdfs_shape = (rows, cols)
    
    def check_shape(self):
        ora_shape = self.ora_shape
        hadoop_shape = self.hdfs_shape
        if ora_shape == hadoop_shape:
            print('Shapes look good')
            return 0 
        else:
            print(f'Oops, shapes are not similar!\n\
            source\'s shape is {ora_shape}\n\
            hadoop\'s shape is {hadoop_shape}')
            return 1
    
    def columns_check(self):
        """Checking sums and row counts for each column"""
        columns_list = [i for i in self.cols_df.collect() if i[1].upper() not in ['CLOB']]
        num_differences = 0
        for i in columns_list:
            ora_col_count = self.ora_checks[i[0].upper()+'_C']
            hdfs_col_count = self.hdfs_checks[i[0].upper()+'_C']
            if int(hdfs_col_count) == int(ora_col_count):
                pass
            elif int(hdfs_col_count) != self.hdfs_checks['TOTAL_COUNT']:
                print(f"""
                Row counts are different!
                Column: {i[0]}
                HDFS row count: {hdfs_col_count}
                Source row count: {ora_col_count}
                """)
                num_differences += 1
            if i[1].upper().startswith('NUMBER') & hdfs_col_count == ora_col_count > 0 or i[1].lower() in self.numeric_columns:
                try:
                    ora_col_sum = self.ora_checks.select(i[0].upper()+'_S').collect()[0][0]
                except AttributeError:
                    ora_col_sum = self.ora_checks[i[0].upper()+'_S']
                try:
                    hdfs_col_sum = self.hdfs_checks.select(i[0].upper()+'_S').collect()[0][0]
                except AttributeError:
                    hdfs_col_sum = self.hdfs_checks[i[0].upper()+'_S']
                scale_factor = 2
                try:
                    if i[3]==None:
                        pass
                    else:
                        scale_factor = int(i[3])
                except:
                    pass
                if ora_col_sum != None:
                    difference = hdfs_col_sum - ora_col_sum
                else:
                    difference = 0
                critical_level = "Not critical"
                tolerance_level = 1 * 10**(-scale_factor)
                if abs(difference) > tolerance_level:
                    critical_level = "CRITICAL"
                if critical_level == "Not critical":
                    continue
                elif critical_level == "CRITICAL":
                    print(f"""
                    Column sums are different!
                    Column: {i[0]}
                    HDFS column sum: {hdfs_col_sum}
                    Source column sum: {ora_col_sum}
                    The difference (hdfs sum - source sum) is: {difference}
                    Critical Level : {critical_level}
                    """)
                    num_differences += 1
        if num_differences == 0:
            print("Row counts and Sums are good")
            return 0
        else:
            return 1
        
    def check(self):
        if self.connection_link.lower().find('oracle') != -1:
            self.get_ora_shape()
        elif self.connection_link.lower().find('postgre') != -1:
            self.get_postgre_shape()
        self.get_hive_shape()
        c_1 = self.check_shape()
        c_2 = self.columns_check()
        return c_1, c_2

    def check_init(self):
        spark.sql(f"REFRESH TABLE {self.hdfs_table_name}")
        c_1, c_2 = self.check()
        c_1
        c_2
        if sum([c_1, c_2]) == 0:
            print('Everything is OK, Nothing to worry about')
        else:
            print('Some things don\'t add up, please check your load again')
            print(f'Last load query was \n {self.ora_query}')
            sys.exit(1)
    
    def check_and_exit(self, path=None):
        if path == None:
            c_1, c_2 = self.check()
            c_1
            c_2
            spark.stop()
            if sum([c_1, c_2]) == 0:
                print('Everything is OK, Nothing to worry about')
                sys.exit(0)
            else:
                print('Some things don\'t add up, please check your load again')
                sys.exit(1)
        elif path != None:
            downloads = 0
            while downloads <= 1:
                if self.connection_link.lower().find('postgre') != -1:
                    driver = 'org.postgresql.Driver'
                elif self.connection_link.lower().find('oracle') != -1:
                    driver = 'org.postgresql.Driver'
                c_1, c_2 = self.check()
                c_1
                c_2
                if sum([c_1, c_2]) == 0:
                    print('Everything is OK, Nothing to worry about')
                    spark.stop()
                    sys.exit(0)
                    break
                elif sum([c_1, c_2]) != 0 and downloads == 0:
                    df = spark.read\
                        .format("jdbc") \
                        .option("url", self.connection_link) \
                        .option("dbtable", f"({self.ora_query}) t") \
                        .option("user", f"{self.username}") \
                        .option("password", f"{self.password}") \
                        .option("driver", driver) \
                        .load()
                    if len(df.head(1)) == 0:
                        print('DataFrame is Empty! Not going to re-download that...')
                        sys.exit(0)
                        break
                    else:
                        print('OVERWRITING PARQUET:', path)
                        df.write.mode('overwrite').parquet(path)
                        downloads += 1
                elif sum([c_1, c_2]) != 0 and downloads > 0 :
                    print('I have re-downloaded the parquet... but:')
                    print('Some things still don\'t add up, please check your load again')
                    sys.exit(1)
                
