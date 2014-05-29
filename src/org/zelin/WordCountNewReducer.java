package org.zelin;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountNewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
//	protected void reduce(Text key, Iterable<IntWritable> values,
//			org.apache.hadoop.mapreduce.Reducer.Context output)
//			throws IOException, InterruptedException {
//		int sum = 0;
//		
//		for(IntWritable iw:values) {
//			sum += iw.get();
//		}
//		
//		output.write(key, new IntWritable(sum));
//	} 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	           int sum = 0;
	           for (IntWritable val : values) {
	               sum += val.get();
	           }
	           context.write(key, new IntWritable(sum));
	       }
}
