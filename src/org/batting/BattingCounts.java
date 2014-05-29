package org.batting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;





public class BattingCounts extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("properties\\hive-site.xml"));
		Path test = new Path("properties\\hive-site.xml");
		System.out.println(test.depth());
		
		String inputTableName = args[0];
		String outputTable = args[1];
		
		Job job = new Job(conf, "BattingCount");
		
		org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(job, "try", inputTableName);
		job.setInputFormatClass(HCatInputFormat.class);
	
		
		
		job.setJarByClass(BattingCounts.class);
		job.setMapperClass(BattingMapper.class);
		job.setReducerClass(BattingReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		
//		job.setOutputFormatClass(TextOutputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		
		job.setOutputFormatClass(HCatOutputFormat.class);
		HCatOutputFormat.setOutput(job, OutputJobInfo.create("try", outputTable, null));
		HCatSchema s = HCatOutputFormat.getTableSchema(job);
		HCatOutputFormat.setSchema(job, s);
		
		
		return (job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BattingCounts(), args);
		System.exit(exitCode);
		
	}
}
