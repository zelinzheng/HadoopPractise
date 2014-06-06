package org.zelin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounts extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");
		
		job.setJarByClass(WordCounts.class);
		
		//job.setMapperClass(WordCountNewMapper.class);
		Configuration confM1 = new Configuration(false);
		ChainMapper.addMapper(job, WordCountNewMapper.class, LongWritable.class, Text.class, 
				Text.class, IntWritable.class, confM1);
		
		Configuration confM2 = new Configuration(false);
		ChainMapper.addMapper(job, WordCountNewMapper2.class,Text.class, IntWritable.class, 
				Text.class, IntWritable.class, confM2);
		
		//job.setCombinerClass(WordCountNewReducer.class);
		//job.setReducerClass(WordCountNewReducer.class);
		Configuration confR1 = new Configuration(false);
		ChainReducer.setReducer(job,WordCountNewReducer.class, Text.class, IntWritable.class, 
				Text.class, IntWritable.class, confR1 );
		
		Configuration confM3 = new Configuration(false);
		ChainReducer.addMapper(job, WordCountNewMapper3.class,Text.class, IntWritable.class, 
				Text.class, IntWritable.class, confM3);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true)? 0:1);
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new WordCounts(), args);
		System.exit(res);
		
	}
	
}
