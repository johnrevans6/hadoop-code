package com.hadoop.relationaljoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelationalJoinReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		
		StringBuilder order_record = new StringBuilder();	
		
		
		//Pull out the order record
		for(Text val : values){
			String[] tmp = val.toString().split(" ");
			
			if(tmp[0].equals("\"order\"")){
				for(String t: tmp){
					order_record.append(t + " ");
				}
				
				break;
			}	
			
		}
		
		//JOIN order record to line_time record				
		for(Text val: values){
			StringBuilder joined_record = new StringBuilder();					
			
			joined_record.append(order_record).append(val.toString());		
			context.write(key, new Text(joined_record.toString()));				
		}
		
			
		
	}
}
