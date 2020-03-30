from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import sys

def find_average(data):
    #map filter
    codeRdd = data.map(lambda s: s.split('\t')[5]).distinct()
    # print(codeRdd.first())
    # code_distinct = codeRdd.collect()
    average = {}
    result = ""
    codes = codeRdd.collect()
    codes.sort()
    for code in codes:
        average[code] = data.filter(lambda s: s.split('\t')[5] == code)\
                   .map(lambda s: float(s.split('\t')[6])).mean()
        result += 'Code ' + str(code) +', average number of bytes = ' + str(average[code]) + '\n'

    # print(average)
    # print(result)
    with open('task1.txt', 'w') as f:
        f.write(result)
    # print(codeRdd.collect())

def find_request(data, spark):
    #join filter
    # #host[0] url[4] timestamp[2]
    # test = data.map(lambda s:(s.split('\t')[0]+s.split('\t')[4],s.split('\t')[2]))
    # print(test.first())
    #
    # test2 = data.map(lambda s:(s.split('\t')[0]+s.split('\t')[4],s.split('\t')[2]))
    # # temp = test.reduceByKey(lambda )
    # # temp = test.join(test2)
    # # print(temp.first())
    # # temp2 = temp.filter(lambda s: 0< s.values()[1] -s.values[0] <3600)
    # # print(temp2.first())
    # tuple1 = data
    # tuple2 = data
    # tuple = print(sc.union([tuple1,tuple2]).first())
    # print(tuple1.join(tuple2).collect())

    data = data.map(lambda s:s.split('\t')).collect()
    print(data)
    temp = spark.createDataFrame(data, schema=['host', 'logname', 'time', 'method', 'URL', 'response', 'bytes', 'referrer', 'useragent'])
    temp.show()
    # employees = spark.createDataFrame(employees, schema=["emp_id", "name", "age"])


if __name__ == '__main__':
    args = sys.argv
    print(args)

    # conf = SparkConf()
    spark = SparkSession.builder.getOrCreate()
    # spark = SparkSession.builder.appName("lz").getOrCreate()
    sc = SparkContext.getOrCreate()

    # sc = SparkContext(conf=conf)
    # data = sc.textFile(args[1])
    data_spark = sc.textFile('file:////Users/buming/Documents/UCR/CS226/SparkRDD/nasa.tsv')
    print(data_spark.first())
    # find_average(data_spark)


    # find_request(data_spark, spark)
    location = 'file:////Users/buming/Documents/UCR/CS226/SparkRDD/nasa.tsv'

    # schema_string = 'host logname time method URL response bytes referrer useragent'


    # data = spark.read.csv(location,)
    # data.show()

    schema_string1 = 'host logname1 time1 method1 URL response1 bytes1 referrer1 useragent1'
    schema_string2 = 'host logname2 time2 method2 URL response2 bytes2 referrer2 useragent2'


    fields1 = list(map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schema_string1.split(" ")))
    schema1 = StructType(fields1)

    fields2 = list(map(lambda fieldName: StructField(fieldName, StringType(), nullable=True), schema_string2.split(" ")))
    schema2 = StructType(fields2)

    rowRDD = data_spark.map(lambda line: line.split('\t')).map(
        lambda attributes: Row(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4], attributes[5],
                               attributes[6], attributes[7], attributes[8]))

    temp1 = spark.createDataFrame(rowRDD, schema1)
    temp2 = spark.createDataFrame(rowRDD, schema2)

    temp1.createOrReplaceGlobalTempView('temp')
    temp2.createOrReplaceGlobalTempView('temp2')
    # temp.show()
    # temp2.show()

    # people.filter(people.age > 30).join(department, people.deptId == department.id) \
    #     .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})

    # temp.join(temp2, temp.host == temp2.host & temp.URL == temp2.URL)
    test = temp1.join(temp2, ['host', 'URL'])
    # test.show()

    # result = spark.sql("SELECT * FROM temp, temp2 WHERE temp.host = temp2.host and temp.URL = temp2.URL")
    #                    # "JOIN temp by(host,URL), temp2 by(host,URL)")
    # result.show()
    result = test.filter(test.time1 - test.time2 > 0).filter(test.time1 - test.time2 <= 3600)
    result.show()
    # file = open('task2.txt', mode='w')
    # global text
    # text = ""
    def f(result):
        # print(result.host)
        file = open('task2.txt', mode='a')
        temp1 = "(\"" + result.host + "\", \"" + result.logname1 + "\", \"" + result.time1 + "\", \"" + result.method1+ "\", \"" + result.URL + "\", \"" + result.response1 + "\", \"" + result.bytes1 + "\", \"" + result.referrer1 + "\", \"" + result.useragent1 + ")"
        temp2 = "(\"" + result.host + "\", \"" + result.logname2 + "\", \"" + result.time2 + "\", \"" + result.method2+ "\", \"" + result.URL + "\", \"" + result.response2 + "\", \"" + result.bytes2 + "\", \"" + result.referrer2 + "\", \"" + result.useragent2 + ")"
        # text = text + temp1 +temp2
        file.write(temp1 + temp2 + '\n')
        file.close()
        print(temp1)
    result.foreach(f)
    # file.close()

    # testDS = spark.read.csv(tsvFileLocation, *sep = "\t")





    # rdd = sc.parallelize(["b", "a", "c"])
    # print(sorted(rdd.map(lambda x: (x, 1)).collect()))
