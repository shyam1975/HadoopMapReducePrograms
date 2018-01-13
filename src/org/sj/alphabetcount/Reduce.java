package org.sj.alphabetcount;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<IntWritable, Text, IntWritable, LongWritable> {
	
	static long counter;
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		counter = 0;
		System.out.println("Entering Reducer ...");
		System.out.println("The key is " + key);
		
		
		for(Text value: values) {

			counter++;
		}
		
		//context.write(key, values);
        context.write(key, new LongWritable(counter));
	}

}
