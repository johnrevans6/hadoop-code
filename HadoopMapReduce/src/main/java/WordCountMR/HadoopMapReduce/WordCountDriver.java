package WordCountMR.HadoopMapReduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountDriver {

 public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();

  Path inputPath = new Path("hdfs://127.0.0.1:9000/input");
  Path outputPath = new Path("hdfs://127.0.0.1:9000/output/wordcount_result");

  JobConf job = new JobConf(conf, WordCountDriver.class);
  job.setJarByClass(WordCountDriver.class);
  job.setJobName("WordCounterJob");

  FileInputFormat.setInputPaths(job, inputPath);
  FileOutputFormat.setOutputPath(job, outputPath);

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  job.setOutputFormat(TextOutputFormat.class);
  job.setMapperClass(WordCountMapper.class);
  job.setReducerClass(WordCountReducer.class);

  FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
  if (hdfs.exists(outputPath))
   hdfs.delete(outputPath, true);

  RunningJob runningJob = JobClient.runJob(job);
  System.out.println("job.isSuccessful: " + runningJob.isComplete());
 }

}
