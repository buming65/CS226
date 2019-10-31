package edu.ucr.cs.cs226.bli147;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.TreeMap;

public class KNN {
    public static class KnnMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            String[] points = value.toString().split(",");

            Double x = Double.valueOf(points[1]);
            Double y = Double.valueOf(points[2]);

            Double XQuery = Double.valueOf(conf.get("XQuery"));
            Double YQuery = Double.valueOf(conf.get("YQuery"));

            Double distance = Math.sqrt(Math.pow((XQuery - x), 2) + Math.pow((YQuery - y), 2));
            context.write(new DoubleWritable(distance), value);
        }
    }

    public static class KnnReducer
            extends Reducer<DoubleWritable, Text, NullWritable, Text>{
        TreeMap<DoubleWritable, Text> knnMap = new TreeMap<>();

        public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            int K = Double.valueOf(conf.get("K")).intValue();
            for(Text val : value){
                //val: value, key: distance
                knnMap.put(key, val);
                if(K > 0){
                    K--;
                    context.getConfiguration().setInt("K", K);
                    String result = "";
                    result += "The distance is " + key.toString() + "\t" + "The point is " + val.toString();
                    context.write(NullWritable.get(), new Text(result));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set("XQuery", args[1]);
        conf.set("YQuery", args[2]);
        conf.set("K", args[3]);

        Job job = Job.getInstance(conf, "KNNMapReduce");
        job.setJarByClass(KNN.class);
        job.setMapperClass(KnnMapper.class);
        job.setReducerClass(KnnReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
