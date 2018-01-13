package org.sj.alphabetcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AlphabetCount extends Configured implements Tool {

	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new AlphabetCount(), args);
		System.exit(res);
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
	
		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);
		
		Configuration conf = getConf();
		Job job = new Job(conf, this.getClass().toString());
		
		FileInputFormat.setInputPaths(job,  inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setJobName("AlphabetCount");		
		job.setJarByClass(AlphabetCount.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0:1;
		
	}
	

}
