In case some error happen, I use sudo command to run these pip and spark-submit command. Because in my environment, I have to use this command to run the script. 

Since I use python and pyspark library to finish this assignment, so in my script I use pip install pyspark first. And in my solution, firstly, I use RDD to find the average number of bytes. Secondly, I convert RDD to DataFrame, and use join and filter to find the specified record.

Just use sudo ./SparkRDD.sh <input-file-path>

