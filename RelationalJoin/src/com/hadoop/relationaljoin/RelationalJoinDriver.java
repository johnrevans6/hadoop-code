package com.hadoop.relationaljoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class RelationalJoinDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "RelationalJoinJob");
		job.setJarByClass(com.hadoop.relationaljoin.RelationalJoinDriver.class);

		job.setMapperClass(RelationalJoinMapper.class);	
		job.setReducerClass(RelationalJoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		Path inputPath = new Path("hdfs://localhost:9000/input/orders");
		Path outputPath = new Path("hdfs://localhost:9000/output/relationaljoin_result");

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		System.exit(ToolRunner.run(conf, new RelationalJoinDriver(), args));
	}

}
