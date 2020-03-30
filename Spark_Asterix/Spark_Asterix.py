from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, FloatType
import time, sys, os
import urllib.request
import urllib.parse
import json

def f_Avg_Spark(x):
    print(x.response, x[1])
    result = 'Code ' + str(x[0]) +', average number of bytes = ' + str(x[1]) + '\r'
    with open('taskA.txt', mode='a') as f:
        f.write(result)
    f.close()

def Spark_Avg(df):
    start = time.time()
    Avg = df.withColumn("bytes", df["bytes"].cast(FloatType())).groupBy('response').mean('bytes').sort('response')
    # Avg.show()
    Avg.foreach(f_Avg_Spark)
    end = time.time()
    print('TaskA: The time of counting average number of bytes for lines of each response code is ' + str((end - start) * 1000) + 'ms')

def get_time():
    #min_timestamp: 804571201
    #max_timestamp: 804572323
    input('Please Input Two TimeStamps, the format should be the Unix Time Stamp \n'
          'You can convert your time to the Unix Time Stamp here https://www.unixtimestamp.com/index.php \n'
          '(Press the enter key to continue)')
    time1 = input('The first tiemstamp (No smaller than 804571201): ')
    if not time1 or time1 < '804571201':
        print('The first time is not right, please do it again.')
        print('Existing')
        sys.exit(1)
    time2 = input('The second timestamp (No bigger than 804572323): ')
    if not time2 or time2 > '804572323':
        print('The second time is not right, please do it again.')
        print('Existing')
        sys.exit(1)
    # print(time1, time2)
    return time1, time2

def Spark_Time(df, time1, time2):
    start = time.time()
    # Time = df.select(df.host, df.time.between('804571206', '804572322'))

    Time = df.filter(df.time >= time1).filter(df.time <= time2)
    # Time = df.filter(df.time >= '804571206').filter(df.time <= '804572322')

    # Time.show()
    result = Time.count()
    txt = 'Spark_SQL: The number of log entries that happen between timestamp ' + str(time1) + ' and ' + str(time2) +' is ' + str(result) + '.\n'
    with open('taskB.txt', mode='a') as f:
        f.write(txt)
    f.close()
    # print(result)
    end = time.time()
    print('TaskB: The time of counting the number of log entries is ' + str((end - start) * 1000) + 'ms')

def Spark_Tesk(input_path):
    spark = SparkSession.builder.master("local").appName("Spark_Asterix").getOrCreate()

    schema_string = 'host logname time method URL response bytes referrer useragent'
    fields = list(
        map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schema_string.split(" ")))
    schema = StructType(fields)

    df = spark.read.csv(input_path, schema=schema, sep='\t')
    # df.show()

    return df


def AsterixDB_Tesk(input_path, time1, time2):
# def AsterixDB_Tesk(input_path):
    start = time.time()
    path = input_path

    # print(path)
    # print(str(path))
    # print('\''+ path+'\'')
    # time1 = '"804571206"'
    # time2 = '"804571233"'

    time1 = '\"' + time1 + '\"'
    time2 = '\"' + time2 + '\"'
    query = 'drop dataverse NasaFile if exists;\n' \
            'create dataverse NasaFile;\n' \
            'use NasaFile;\n' \
            'create type NasaType as closed{' \
            '    host:string,' \
            '    logname:string,' \
            '    time:string,' \
            '    method:string,' \
            '    URL:string,' \
            '    response:string,' \
            '    bytes:string,' \
            '    referrer:string,' \
            '    useragent:string};\n' \
            'create external dataset Nasaitem(NasaType) using localfs ' \
            '(("path" = "127.0.0.1://' + path + '"), ("format"="delimited-text"), ("delimiter"="\t"));' \
            'Select count(*)' \
            'from Nasaitem item ' \
            'where item.time >= ' + time1 + ' and item.time <= ' + time2 + ' ;'
    host = 'http://localhost:19002/query/service'
    data = dict()
    data['statement'] = query
    data = urllib.parse.urlencode(data).encode('utf-8')
    with urllib.request.urlopen(host, data) as handler:
        result = json.loads(handler.read())
        df = json.dumps(result['results'])
        # print(df)
        result = json.loads(df, object_hook=dict)[0]['$1']
        # print(result)
        # print(result[0]['$1'])
    txt = 'Asterix: The number of log entries that happen between timestamp ' + str(time1) + ' and ' + str(time2) + ' is ' + str(
        result) + '.\n'
    with open('taskC.txt', mode='a') as f:
        f.write(txt)
    f.close()
    end = time.time()
    print('TaskC: The time of counting the number of log entries is ' + str((end - start) * 1000) + 'ms')



if __name__ == '__main__':

    args = sys.argv
    print(args)

    input_path = args[1]
    #get the df
    df = Spark_Tesk(input_path)
    # df.sort('time').show()
    # print(df.orderBy(df.time.desc()).collect())

    #count the avg of bytes
    Spark_Avg(df)

    #count the number of log entries between two timestamps by SparkSQL
    time1, time2 = get_time()
    Spark_Time(df, time1, time2)

    #count the number of log entries between two timestamps by AsterixDB
    AsterixDB_Tesk(input_path, time1, time2)



