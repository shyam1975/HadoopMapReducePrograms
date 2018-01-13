package org.sj.maxtemp;

import java.io.IOException;
import java.util.StringTokenizer; 
 


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemp {
	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	Text year = new Text();  	
    	IntWritable temp = new IntWritable();     
        @Override 
        public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
        	
        	String line = value.toString(); 
        	StringTokenizer tokenizer = new StringTokenizer(line," ");  
            while (tokenizer.hasMoreTokens()) {            	

            	String yearString= tokenizer.nextToken();
            	year.set(yearString);
            	Integer tempInt= Integer.parseInt(tokenizer.nextToken());
            	temp.set(tempInt);            	
                context.write(year, temp); 
            } 
        } 
    } 
    
 
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
       
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {        	
            int maxTemp = -200;  //TODO, better way to find the max temp?             
            for(IntWritable x : values)
            {
            	if(x.get() > maxTemp)
            		maxTemp = x.get();
            }         
                      
            context.write(key, new IntWritable(maxTemp)); 
        } 
 
    }      	
    
    public static void main(String[] args) throws Exception {     	
		
		Configuration conf = new Configuration();			
		Job job = new Job(conf, "MaxTemp");			
		job.setJarByClass(MaxTemp.class);			
		job.setMapperClass(Map.class);					
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);				
		job.setOutputKeyClass(Text.class);				
		job.setOutputValueClass(IntWritable.class);					
		job.setInputFormatClass(TextInputFormat.class);					
		job.setOutputFormatClass(TextOutputFormat.class);     
		
       	        Path outputPath = new Path(args[1]);	
        
        	FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    } 

}
