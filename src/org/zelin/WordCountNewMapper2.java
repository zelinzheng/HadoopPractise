package org.zelin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountNewMapper2 extends
		Mapper<Text, IntWritable, Text, IntWritable> {

	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();
	//
	// protected void map(LongWritable key, Text value, Context context)
	// throws IOException, InterruptedException {
	//
	// String line = value.toString();
	// StringTokenizer tokenizer = new StringTokenizer(line);
	//
	// while(tokenizer.hasMoreTokens()){
	// word.set(tokenizer.nextToken());
	// context.write(word, one);
	// }
	// }

	// private final static IntWritable one = new IntWritable(1);

	public void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		String originalkey = key.toString();
		if (originalkey.length() > 4) {
			originalkey = originalkey.substring(0, 4);
		}
		context.write(new Text(originalkey), value);
	}

}
