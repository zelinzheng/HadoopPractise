package org.batting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import  org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;

public class BattingMapper extends Mapper<Object, HCatRecord, Text, IntWritable>{
	
	Integer run;
	Text text;
	
	protected void map(Object key, HCatRecord value, Context context)
			throws IOException, InterruptedException {
		
		
		System.out.println("-----------------Map begin-------------------");
		
		HCatSchema schema = HCatBaseInputFormat.getTableSchema(context);
		
		run = value.getInteger("runs", schema);
		if(run == null) run= new Integer(0);
		text = new Text( value.getString("player_id", schema));
		context.write(text, new IntWritable(run));
	}
}
