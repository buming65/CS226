# UCR CS226

## Assignment 1

➜  ~ hadoop jar HDFSUpload.jar ucr.edu.cs.cs226.bli147.HDFSUpload AREAWATER.csv /test

##Description
Write a Java program that copies a file from the local machine to HDFS. It should run from the command line and take two command line arguments, a path to a local file, and a path in HDFS. It should copy the file from the local file system to HDFS. If the local file does not exist, it should signal an error. If the file in HDFS already exists, it should report this and fail. If the file in HDFS cannot be created, for any reason, this should also be reported. Test your program on your local machine in the pseudo-distributed Hadoop installation. You should not use the methods FileSystem#copyFromLocal or FileSystem#moveFromLocal in your implementation. Rather, you should use the methods described in class such as FileSystem#open and FileSystem#create.

Use the following three tasks to measure the performance of the file system and compare the performance of the LocalFileSystem to the DistributedFileSystem.

1. The total time for copying the 2GB file provided in the instructions below.
2. The total time for reading a 2GB sequentially from the start to the end.
3. The total time to make 2,000 random accesses, each of size 1KB. To test this, generate a random
position in the file, seek to that position, and read 1 KB.