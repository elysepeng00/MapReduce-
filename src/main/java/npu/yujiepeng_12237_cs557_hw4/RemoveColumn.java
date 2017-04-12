/*
 * Student Info: Name=Yujie Peng, ID=12237
 * Subject: CS570_HW4_Full_2016
 * Author: yujie
 * Filename: RemoveCloumn.java
 * Date and Time: Nov 16, 2016 7:20:49 PM
 * Project Name: YujiePeng_12237_CS557_HW4
 */
package npu.yujiepeng_12237_cs557_hw4;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class RemoveColumn {
    private static int lineSize = 0;
    static ArrayList<ArrayList<String>> collection = new  ArrayList<ArrayList<String>>();
    public static class RemoveMapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {
        
        HashSet<Integer> columnSet = new HashSet<Integer>();
        
        /** in map(), for loop each line:
         * store each value into ArrayList: lineList, store index with value equals to NaN into columnSet
         * store each lineList into ArrayList<ArrayList<String>>: collection
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               // System.out.println("========in mapper========="); 
               
                ArrayList<String> lineList = new ArrayList<String>();
                String[] lines = value.toString().split("\\s+");
                for ( int i = 0; i < lines.length;i++){ 
                    if ( lines[i].equals("NaN")) columnSet.add(i);
                    lineList.add(lines[i]);              
                }
                collection.add(lineList);
//                System.out.println("collection.size() = "+collection.size());
//                
//                for ( int i = 0; i< collection.size(); i++){
//                    System.out.println(collection.get(i).toString());   
//                }
//                
//                for ( int s : columnSet){
//                    System.out.println("columnSet="+ s); 
//                }
        }

        @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("========in cleanup=======");
//            for ( int i = 0; i< collection.size(); i++){
//                    System.out.println(collection.get(i).toString());   
//                }
//            for ( int s : columnSet){
//                    System.out.println("columnSet="+ s); 
//                } 
              System.out.println("columnSet.size() =" + columnSet.size());

             /** delete the columns with NaN value:
              * for loop collection, and for loop each element curList, 
              * delete element from curList with index in columnSet
              */
             String removed  = "";
            for ( int i = 0; i < collection.size(); i++){
                ArrayList<String> curList = collection.get(i);
                for (int j= curList.size(); j >= 0; j--){
                    if ( columnSet.contains(j)) {
                        removed = curList.remove(j); 
                    }
                    //System.out.println("In curList[" + i +"]"+ "curList.remove =" + removed);
                }   
            }
            StringBuilder sb = null;
             for ( int i = 0; i< collection.size(); i++){
                    ArrayList<String> curList = collection.get(i);
                    lineSize = curList.size();
                    //System.out.println(collection.get(i).toString()); 
                    sb = new StringBuilder();
                    for ( String s : curList){        
                        sb.append(s + " ");
                    }
                    context.write(NullWritable.get(), new Text (sb.toString()));
                }
            
	}
}

    public static class RemoveReducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {
        HashSet<Integer> columnSet = new HashSet<Integer>();
        
        
        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            System.out.println("========in reduce=========");
            for ( int i = 0; i< lineSize; i++){
                columnSet.add(i);
            }
            
            String[] preline = null;
            String[] curline = null;
            int count = 0;           
           
           /** Get column which will be deleted: find constant value columns
            * Compare preline and curLine, if same column have different value, remove the index from columnSet
            * The columns remain in columnSet will be deleted.
            */
            for (Text v : values) {
                if (count == 0){
                    curline = v.toString().split("\\s+");
                    preline = Arrays.copyOf(curline, curline.length);
                    count++;
                }else{
                    curline = v.toString().split("\\s+");
                     for ( int i = 0; i< curline.length; i++){  
                        if (!preline[i].equals(curline[i])) columnSet.remove(i);
                    } 
                }
            }
            /**repeat the same step in cleanup(): 
             * delete the columns with index in columnSet, delete constant value 
             */
            
            System.out.println("columnSet.size() =" + columnSet.size());
            String removed  = "";
            for ( int i = 0; i < collection.size(); i++){
                ArrayList<String> curList = collection.get(i);
                for (int j= curList.size(); j >= 0; j--){
                    if ( columnSet.contains(j)) {
                        removed = curList.remove(j); 
                    }
                }   
            }
            StringBuilder sb = null;
             for ( int i = 0; i< collection.size(); i++){
                    ArrayList<String> curList = collection.get(i);
                   
                    //System.out.println(collection.get(i).toString());  
                    sb = new StringBuilder();
                    for ( String s : curList){        
                        sb.append(s + " ");
                    }
                    context.write(NullWritable.get(), new Text (sb.toString()));
                }
        }
    }
    
   public static void main(String[] args) throws Exception {
		java.nio.file.Path path = Paths.get("removeColumns_output");
		if (Files.exists(path)) {
			FileUtils.deleteDirectory(path.toFile());
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "remove columns");
		job.setJarByClass(RemoveColumn.class);
		job.setMapperClass(RemoveMapper.class);
		job.setReducerClass(RemoveReducer.class);
		job.setNumReduceTasks( 1 );
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));  // mergeyeild_input
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// removeColumns_output
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}