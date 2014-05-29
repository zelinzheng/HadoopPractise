package org.batting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
public class BattingReducer extends Reducer<Text, IntWritable, Object, HCatRecord>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
		Context context)
		throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable i:values) {
			sum += i.get();
		}
		List<HCatFieldSchema> schemaList = new ArrayList<HCatFieldSchema>();
		
		schemaList.add(new HCatFieldSchema("player_id", Type.STRING, null));
		schemaList.add(new HCatFieldSchema("runs", Type.STRING, null));
		
		HCatSchema schema = new HCatSchema(schemaList);
		HCatRecord record = new DefaultHCatRecord(2); 
		record.set("player_id", schema, key.toString());
		record.setInteger("runs", schema, sum);
		context.write(null, record);
	}
	

}
