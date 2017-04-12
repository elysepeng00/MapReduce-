/*
 * Student Info: Name=Yujie Peng, ID=12237
 * Subject: CS570_HW4_Full_2016
 * Author: yujie
 * Filename: YieldData.java
 * Date and Time: Nov 15, 2016 10:51:26 AM
 * Project Name: YujiePeng_12237_CS557_HW4
 */
package npu.yujiepeng_12237_cs557_hw4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author yujie
 */
public class YieldData extends Configured implements Tool {

    public static class MyMapper extends
            Mapper<LongWritable, Text, NullWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                //System.out.println("========in yeild mapper=========");  
                String[] lines = value.toString().split("\\s+");
//                for ( int i = 0; i < lines.length;i++){
//                    System.out.println(lines[i]);
//                }
                String failed = lines[0];
               // System.out.println("lines[0] =: "+lines[0]);
                if (Integer.parseInt(failed) == 1){  
                    //System.out.println("value = "+value.toString());
                    context.write(NullWritable.get(), new Text(value));
                }
            }
        }

    public static class MyReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            System.out.println("========in reduce=========");
            
            String preValue= "";
            String curValue = "";
            int count = 0;
            for (Text v : values) {
                StringBuilder out = new StringBuilder();
                String month = v.toString();
                month = month.substring(month.indexOf("/")+1);
                month = month.substring(0, month.indexOf("/"));
                if (month.charAt(0)=='0'){
                   month = month.substring(month.indexOf("0")+1);
                }
                 System.out.println("valueStr =: "+month);
                 curValue = month;
                 if ( curValue.equals(preValue)) count++;
                 else if (count == 0) count = 1;
                 else {
                     out.append(preValue + ": " + count);
                     context.write(NullWritable.get(), new Text(out.toString()));
                     count = 1;
                 }
                 preValue = curValue;                    
                }                 
                 context.write(NullWritable.get(), new Text(curValue.toString() + ": " + count ));
            }
        }

    

    @Override
     public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(YieldData.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.newInstance(getConf());
        Path outputFilePath = new Path(args[1]);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new YieldData(), args);
        System.exit(res);
    }   
}