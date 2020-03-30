# ASSIGNMENT 4

### Run the Script

To run this script, please input the command like ./Spark_Asterix.sh [input_file_path]

For example: ./Spark_Asterix.sh XXX/Spark_Asterix/nasa.tsv

### SOME NOTICES

1. My Programming assumes that the input_file_path is on the local machine to ensure the program part '(("path" = "127.0.0.1://' + path + '"), ("format"="delimited-text"), ("delimiter"="\t"));'  in AsterixDB_Tesk could run rightly.

2. Since the instruction on the assignment4 indicated that we count the number of log entries that happen between two given timestamps, so in my programming, the user need to input the two timestamps in the Unix Time Stamp format, and the timestamps should also be no smaller than 804571201 and no bigger than 804572323.

3. Here are the examples of the three task.txt

4. TASKA.txt: 

   ```
   Code 200, average number of bytes = 22848.716743119265
   Code 302, average number of bytes = 73.54901960784314
   Code 304, average number of bytes = 0.0
   Code 404, average number of bytes = 0.0
   ```

5. TASKB.txt

```
Spark_SQL: The number of log entries that happen between timestamp 804571206 and 804571233 is 25.
```

6. TASKC.txt

```
Asterix: The number of log entries that happen between timestamp "804571206" and "804571233" is 23.
```

