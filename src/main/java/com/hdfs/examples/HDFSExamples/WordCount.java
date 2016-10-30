package com.hdfs.examples.HDFSExamples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class WordCount extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
    	  Configuration hconf = new Configuration();
            //creating a JobConf object and assigning a job name for identification purposes
    	  Path maprfsCoreSitePath = new Path(
  				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/core-site.xml");
  		Path hdfsSitePath = new Path(
  				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/hdfs-site.xml");
  		Path mapredSitePath = new Path(
  				"/Users/hadoop/hadoop-2.5.2/etc/hadoop/mapred-site.xml");

  		// Add the resources to Configuration instance
  		hconf.addResource(maprfsCoreSitePath);
  		hconf.addResource(hdfsSitePath);
  		hconf.addResource(mapredSitePath);
  		
            JobConf conf = new JobConf(hconf, WordCount.class);
            conf.setJobName("WordCount");
            conf.setJar("MRExamples-jar-with-dependencies.jar");
            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            
            //Providing the mapper and reducer class names
            //conf.setMapperClass(WordCountMapper.class);
            //conf.setReducerClass(WordCountReducer.class);
            //We wil give 2 arguments at the run time, one in input path and other is output path
            Path inp = new Path("/user/hadoop.txt");
            Path out = new Path("/user/output/");
            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(conf, inp);
            FileOutputFormat.setOutputPath(conf, out);

            JobClient.runJob(conf);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
            // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new WordCount(),args);
            System.exit(res);
      }
}
