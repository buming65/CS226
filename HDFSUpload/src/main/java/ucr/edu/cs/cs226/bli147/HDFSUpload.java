package ucr.edu.cs.cs226.bli147;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.Random;
import java.util.RandomAccess;
import java.util.concurrent.TimeUnit;

public class HDFSUpload {
    public static void main(String args[]) throws IOException{
        if(args.length < 2){
            System.out.println("ERROR : Please enter the configurations " +
                    "of input_path and output_path");
            System.out.println("EXITING");
            System.exit(1);
        }
        String local_path_read = args[0];
        File local_file = new File(local_path_read);
        //Check whether the local file exists
        if(!local_file.exists()){
            System.out.println("ERROR : The local file does not exist");
            System.out.println("EXITING");
            System.exit(1);
        }

        String hdfs_path_read = args[1];
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path hdfs_path = new Path(hdfs_path_read);

        //Check whether the hdfs file exists
        if(fs.exists(hdfs_path)){
            System.out.println("ERROR : The Hdfs path already exists");
            System.out.println("EXITING");
            System.exit(1);
        }

        //Copy file
        //1. Copy file from local to local
        long start_time1 = System.currentTimeMillis();
        FileChannel in1 = new FileInputStream(local_file).getChannel();
        FileChannel out1 = new FileOutputStream(new File("local_copy.csv")).getChannel();
        out1.transferFrom(in1, 0, in1.size());
        in1.close();
        out1.close();
        long end_time1 = System.currentTimeMillis();
        double total_time1 = (end_time1 - start_time1)/1000.0;
        System.out.println("1. It takes "+total_time1+" seconds to copy the local file to the local.");

        //2. Copy file from local to hdfs
        long start_time2 = System.currentTimeMillis();
        InputStream in2 = new BufferedInputStream(new FileInputStream(local_path_read));
        OutputStream out2 = fs.create(hdfs_path);
        IOUtils.copyBytes(in2, out2, config);
        in2.close();
        out2.close();
        long end_time2 = System.currentTimeMillis();
        double total_time2 = (end_time2 - start_time2)/1000.0;
        System.out.println("2. It takes "+total_time2+" seconds to copy the local file to the hdfs.");

        //Read file
        //3. Read file from local
        long start_time3 = System.currentTimeMillis();
        FileInputStream in3 = new FileInputStream(local_path_read);
        BufferedReader buffer3 = new BufferedReader(new InputStreamReader(in3));
        try{
            String str3 = null;
            while ((str3 = buffer3.readLine()) != null){
                continue;
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        long end_time3 = System.currentTimeMillis();
        double total_time3 = (end_time3 - start_time3)/1000.0;
        System.out.println("3. It takes "+total_time3+" seconds to read the file from local.");

        //4. Read file from hdfs
        long start_time4 = System.currentTimeMillis();
        FSDataInputStream in4 = fs.open(hdfs_path);
        BufferedReader buffer4 = new BufferedReader(new InputStreamReader(in4));
        try{
            String str4 = null;
            while ((str4 = buffer4.readLine()) != null){
                continue;
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        in4.close();
        long end_time4 = System.currentTimeMillis();
        double total_time4 = (end_time4 - start_time4)/1000.0;
        System.out.println("4. It takes "+total_time4+" seconds to read the file from hdfs.");

        //Random accesses
        //5. Random accesses from local
        long start_time5 = System.currentTimeMillis();
        int i5 = 2000;
        int min5 = 0;
        int max5 = (int)local_file.length();
        if(max5 < 0){
            max5 = max5 * (-1);
        }
        while (i5 > 0){
            Random random5 = new Random();
            int offset5 = random5.nextInt(max5)%(max5-min5+1) + min5;
            RandomAccessFile file5 = new RandomAccessFile(local_file,"r");
            file5.seek(offset5);
            byte[] bytes5 = new byte[1024];
            file5.read(bytes5);
            i5--;
        }
        long end_time5 = System.currentTimeMillis();
        double total_time5 = (end_time5 - start_time5)/1000.0;
        System.out.println("5. It takes "+total_time5+" seconds to seek random 2000 accesses to the local file.");

        //6. Random accesses from hdfs
        long start_time6 = System.currentTimeMillis();
        int i6 = 2000;
        int min6 = 0;
        int max6 = (int) (fs.getFileStatus(hdfs_path).getLen());
        if(max6 < 0){
            max6 = max6 * (-1);
        }
        FSDataInputStream in6 = fs.open(hdfs_path);
        while (i6 > 0){
            Random random6 = new Random();
            int offset6 = random6.nextInt(max6)%(max6-min6+1) + min6;
            in6.seek(offset6);
            byte[] bytes6 = new byte[1024];
            in6.read(offset6, bytes6, 0,1024);
            i6--;
        }
        long end_time6 = System.currentTimeMillis();
        double total_time6 = (end_time6 - start_time6)/1000.0;
        System.out.println("6. It takes "+total_time6+" seconds to seek random 2000 accesses to the hdfs file.");


        System.out.println("THE TOTAL INFORMATION:");
        System.out.println("1. It takes "+total_time1+" seconds to copy the local file to the local.");
        System.out.println("2. It takes "+total_time2+" seconds to copy the local file to the hdfs.");
        System.out.println("3. It takes "+total_time3+" seconds to read the file from local.");
        System.out.println("4. It takes "+total_time4+" seconds to read the file from hdfs.");
        System.out.println("5. It takes "+total_time5+" seconds to seek random 2000 accesses to the local file.");
        System.out.println("6. It takes "+total_time6+" seconds to seek random 2000 accesses to the hdfs file.");
    }
}
