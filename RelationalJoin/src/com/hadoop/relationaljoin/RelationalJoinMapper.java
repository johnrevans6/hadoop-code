package com.hadoop.relationaljoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelationalJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split(",");
		
		
		//Extract the primary key of each record
		String id = line[1];
		
		StringBuilder record = new StringBuilder();
		
		//Reassemble the record as a single concatenated string
		for(String r:line){
			record.append(r);
		}
		
		//emit key and record
		context.write(new Text(id), new Text(record.toString()));;

	}

}
