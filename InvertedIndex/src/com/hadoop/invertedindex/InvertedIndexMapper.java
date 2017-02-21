package com.hadoop.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Get the name of the file
		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		String line = value.toString();
		//Tokenize the words in the line
		String words[] = line.split(" ");
		//For each word in words, emit the word as a key and the filename as the value
		for(String word:words){
			
			context.write(new Text(word), new Text(filename));
		}
	}

}
