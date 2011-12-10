package org.commoncrawl.tutorial;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;

public class HelloWorld {
  private static final String CC_BUCKET = "common-crawl-002";
  
  public static void main(String[] args) throws IOException {
    String awsCredentials = args[0];
    String awsSecret = args[1];
    String inputPrefixes = args[2];
    String outputFile = args[3];
    
    JobConf conf = new JobConf();

    // Connect this job to the CommonCrawl S3 repository
    System.out.println("using awsCredentials:" + awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
    conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);
    System.out.println("using input prefixes:" + inputPrefixes);
    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
    
    ARCInputFormat inputFormat = new ARCInputFormat();
    FileOutputFormat.setOutputPath(conf, new Path(outputFile));
    inputFormat.configure(conf);
    
    conf.setMapperClass(WordCountMapper.class);
    conf.setReducerClass(WordCountReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    JobClient.runJob(conf);
  }
}