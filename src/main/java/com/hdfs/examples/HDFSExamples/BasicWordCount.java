package com.hdfs.examples.HDFSExamples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BasicWordCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// public static void main(String[] args) throws Exception {
	// Configuration conf = new Configuration();
	// Path maprfsCoreSitePath = new Path(
	// "/Users/hadoop/hadoop-2.5.2/etc/hadoop/core-site.xml");
	// Path hdfsSitePath = new Path(
	// "/Users/hadoop/hadoop-2.5.2/etc/hadoop/hdfs-site.xml");
	// Path mapredSitePath = new Path(
	// "/Users/hadoop/hadoop-2.5.2/etc/hadoop/mapred-site.xml");
	//
	// // Add the resources to Configuration instance
	// conf.addResource(maprfsCoreSitePath);
	// conf.addResource(hdfsSitePath);
	// conf.addResource(mapredSitePath);
	//
	// Job job = new Job(conf, "wordcount");
	// job.setJarByClass(BasicWordCount.class);
	// // job.setJar("wordcount.jar");
	// job.setOutputKeyClass(Text.class);
	// job.setOutputValueClass(IntWritable.class);
	//
	// job.setMapperClass(TokenizerMapper.class);
	// job.setReducerClass(IntSumReducer.class);
	//
	// job.setInputFormatClass(TextInputFormat.class);
	// job.setOutputFormatClass(TextOutputFormat.class);
	//
	// FileInputFormat.addInputPath(job, new Path("/user/hadoop.txt"));
	// FileOutputFormat.setOutputPath(job, new Path("/user/output"));
	//
	// job.waitForCompletion(true);
	// }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path maprfsCoreSitePath = new Path(
				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/core-site.xml");
		Path hdfsSitePath = new Path(
				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/hdfs-site.xml");
		Path mapredSitePath = new Path(
				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/mapred-site.xml");

		// Add the resources to Configuration instance
		conf.addResource(maprfsCoreSitePath);
		conf.addResource(hdfsSitePath);
		conf.addResource(mapredSitePath);
		Job job = new Job(conf, "WordCount");
		job.setJar("/Users/sekharreddy/work/scala_ide_workspace/MRExamples/target/MRExamples-jar-with-dependencies.jar");
		FileInputFormat.addInputPath(job, new Path("/user/hadoop.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/output"));

		job.setJarByClass(BasicWordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileSystem hdfs = FileSystem.get(conf);
		  if (hdfs.exists(new Path("/user/output")))
		      hdfs.delete(new Path("/user/output"), true);
		  
		 int code = job.waitForCompletion(true) ? 0 : 1;
		    System.exit(code);
	}

}
