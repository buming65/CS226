from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import sys
import os
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home'


def find_average(data):
    #map filter
    codeRdd = data.map(lambda s: s.split('\t')[5]).distinct()
    average = {}
    result = ""
    codes = codeRdd.collect()
    codes.sort()
    for code in codes:
        average[code] = data.filter(lambda s: s.split('\t')[5] == code)\
                   .map(lambda s: float(s.split('\t')[6])).mean()
        result += 'Code ' + str(code) +', average number of bytes = ' + str(average[code]) + '\r'
    with open('task1.txt', 'w') as f:
        f.write(result)

def f(result):
    file = open('task2.txt', mode='a')
    # temp1 = "(\"" + result.host + "\", \"" + result.logname1 + "\", \"" + result.time1 + "\", \"" + result.method1 + "\", \"" + result.URL + "\", \"" + result.response1 + "\", \"" + result.bytes1 + "\", \"" + result.referrer1 + "\", \"" + result.useragent1 + ")"
    temp1 = result.host + "\t" + result.logname1 + "\t" + result.time1 + "\t" + result.method1 + "\t" + result.URL + "\t" + result.response1 + "\t" + result.bytes1 + "\t" + result.referrer1 + "\t" + result.useragent1 + "\t"
    temp2 = result.host + "\t" + result.logname2 + "\t" + result.time2 + "\t" + result.method2 + "\t" + result.URL + "\t" + result.response2 + "\t" + result.bytes2 + "\t" + result.referrer2 + "\t" + result.useragent2 + "\t"
    file.write(temp1 + temp2 + '\r')
    file.close()

def find_request(data, spark):
    #join filter
    schema_string1 = 'host logname1 time1 method1 URL response1 bytes1 referrer1 useragent1'
    schema_string2 = 'host logname2 time2 method2 URL response2 bytes2 referrer2 useragent2'

    fields1 = list(
        map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schema_string1.split(" ")))
    schema1 = StructType(fields1)

    fields2 = list(
        map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schema_string2.split(" ")))
    schema2 = StructType(fields2)

    rowRDD = data.map(lambda line: line.split('\t')).map(
        lambda attributes: Row(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4], attributes[5],
                               attributes[6], attributes[7], attributes[8]))

    temp1 = spark.createDataFrame(rowRDD, schema1)
    temp2 = spark.createDataFrame(rowRDD, schema2)

    temp1.createOrReplaceGlobalTempView('temp')
    temp2.createOrReplaceGlobalTempView('temp2')

    test = temp1.join(temp2, ['host', 'URL'])

    result = test.filter(test.time1 - test.time2 > 0).filter(test.time1 - test.time2 <= 3600)
    # result.show()

    result.foreach(f)


if __name__ == '__main__':
    args = sys.argv
    print(args)

    spark = SparkSession.builder.master("local").appName("SparkRDD").getOrCreate()
        # .config("spark.some.config.option", "some-value") \


    sc = SparkContext.getOrCreate()
    # data_spark = sc.textFile('file:////Users/buming/Documents/UCR/CS226/SparkRDD/nasa.tsv')
    data_spark = sc.textFile(args[1])
    find_average(data_spark)
    find_request(data_spark, spark)
