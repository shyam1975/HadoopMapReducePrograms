package org.sj.wordcount;

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

public class WordCount {
	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {
    	
    	Text word = new Text();  	
    	IntWritable one = new IntWritable(1);     
        @Override 
        public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
        	
        	String line = value.toString(); 
        	StringTokenizer tokenizer = new StringTokenizer(line," ");  
            while (tokenizer.hasMoreTokens()) {            	
            	String wordString = tokenizer.nextToken();
            	word.set(wordString);
            	            	
                context.write(word, one); 
            } 
        } 
    } 
    
 
	public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    	
       
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {        	
            int sum = 0;             
            for(IntWritable x : values)
            {
            	sum++;
            }         
                      
            context.write(key, new LongWritable(sum)); 
        } 
 
    }      	
    
    public static void main(String[] args) throws Exception {     	
		
		Configuration conf = new Configuration();			
		Job job = new Job(conf, "WordCount");			
		job.setJarByClass(WordCount.class);			
		job.setMapperClass(Map.class);					
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);				
		job.setOutputKeyClass(Text.class);				
		job.setOutputValueClass(LongWritable.class);					
		
		job.setInputFormatClass(TextInputFormat.class);					
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		
       	        Path outputPath = new Path(args[1]);	
        
        	FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    } 

}
