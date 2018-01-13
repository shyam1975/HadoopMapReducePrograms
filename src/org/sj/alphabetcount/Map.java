package org.sj.alphabetcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private Text word = new Text();
	
	//private Logger logger = Logger.getLogger(Map.class);

	@Override
	public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(new IntWritable(word.getLength()), word);
			
			System.out.println(""+word.getLength() + " " + word);
		} //end while
		
	} //end method
} //end class
