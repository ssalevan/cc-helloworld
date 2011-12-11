package org.commoncrawl.tutorial;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;

public class HelloWorld {
  private static final String CC_BUCKET = "commoncrawl-crawl-002";
  
  public static void main(String[] args) throws IOException {
    // Parses command-line arguments.
	String awsCredentials = args[0];
    String awsSecret = args[1];
    String inputPrefixes = args[2];
    String outputFile = args[3];
    
    // Echoes back command-line arguments.
    System.out.println("Using AWS Credentials: " + awsCredentials);
    System.out.println("Using S3 bucket paths: " + inputPrefixes);
    
    // Creates a new Job Configuration for this Hadoop job
    JobConf conf = new JobConf();

    // Configures this job with your Amazon AWS credentials
    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
    conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);
    
    // Configures where the input comes from when running our Hadoop job,
    // in this case, gzipped ARC files from the specified Amazon S3 bucket
    // paths.
    ARCInputFormat inputFormat = new ARCInputFormat();
    inputFormat.configure(conf);
    conf.setInputFormat(ARCInputFormat.class);
    
    // Configures what kind of Hadoop output we want.
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    // Configures where the output goes to when running our Hadoop job.
    FileOutputFormat.setOutputPath(conf, new Path(outputFile));

    // Tells the user some context about this job.
    InputSplit[] splits = inputFormat.getSplits(conf, 0);
    if (splits.length == 0) {
      System.out.println("ERROR: No .ARC files found!");
      return;
    }
    System.out.println("Found " + splits.length + " InputSplits:");
    for (InputSplit split : splits) {
        System.out.println(" - will process file: " + split.toString());
    }
    
    // Tells Hadoop what Mapper and Reducer classes to use.
    conf.setMapperClass(WordCountMapper.class);
    conf.setReducerClass(WordCountReducer.class);
    
    // Runs the job!
    JobClient.runJob(conf);
  }
}