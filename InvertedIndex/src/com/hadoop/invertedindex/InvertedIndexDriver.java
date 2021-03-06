package com.hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class InvertedIndexDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception{
		Job job = Job.getInstance(getConf(), "InvertedIndexJob");
		
		job.setJarByClass(InvertedIndexDriver.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/inverted_index_result");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		System.exit(ToolRunner.run(conf, new InvertedIndexDriver(), args));
	}

}
