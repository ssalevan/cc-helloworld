package org.commoncrawl.tutorial;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.Progressable;

import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;

/**
 * Simple word-counting example of how to access the CommonCrawl dataset.
 * 
 * @author Steve Salevan <steve.salevan@gmail.com>
 */
public class HelloWorld {
  /**
   * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
   */
  private static final String CC_BUCKET = "aws-publicdatasets";
  
  /**
   * Outputs counted words into a CSV-formatted file.
   */
  public static class CSVOutputFormat
      extends TextOutputFormat<Text, LongWritable> {
    public RecordWriter<Text, LongWritable> getRecordWriter(
        FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new CSVRecordWriter(fileOut);
    }

    protected static class CSVRecordWriter
        implements RecordWriter<Text, LongWritable> {
      protected DataOutputStream outStream;

      public CSVRecordWriter(DataOutputStream out) {
        this.outStream = out;
      }

      public synchronized void write(Text key, LongWritable value)
          throws IOException {
        CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
        csvOutput.writeString(key.toString(), "word");
        csvOutput.writeLong(value.get(), "occurrences");
      }

      public synchronized void close(Reporter reporter) throws IOException {
        outStream.close();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    // Parses command-line arguments.
    String awsCredentials = args[0];
    String awsSecret = args[1];
    String inputPrefixes = "common-crawl/crawl-002/" + args[2];
    String outputFile = args[3];

    // Echoes back command-line arguments.
    System.out.println("Using AWS Credentials: " + awsCredentials);
    System.out.println("Using S3 bucket paths: " + inputPrefixes);

    // Creates a new job configuration for this Hadoop job.
    JobConf conf = new JobConf();

    // Configures this job with your Amazon AWS credentials
    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
    conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);

    // Configures where the input comes from when running our Hadoop job,
    // in this case, gzipped ARC files from the specified Amazon S3 bucket
    // paths.
    ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
    ARCInputFormat inputFormat = new ARCInputFormat();
    inputFormat.configure(conf);
    conf.setInputFormat(ARCInputFormat.class);

    // Configures what kind of Hadoop output we want.
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);

    // Configures where the output goes to when running our Hadoop job.
    CSVOutputFormat.setOutputPath(conf, new Path(outputFile));
    CSVOutputFormat.setCompressOutput(conf, false);
    conf.setOutputFormat(CSVOutputFormat.class);

    // Allows some (10%) of tasks fail; we might encounter the 
    // occasional troublesome set of records and skipping a few 
    // of 1000s won't hurt counts too much.
    conf.set("mapred.max.map.failures.percent", "10");

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

    // Tells Hadoop what Mapper and Reducer classes to use;
    // uses combiner since wordcount reduce is associative and commutative.
    conf.setMapperClass(WordCountMapper.class);
    conf.setCombinerClass(WordCountReducer.class);
    conf.setReducerClass(WordCountReducer.class);

    // Tells Hadoop mappers and reducers to pull dependent libraries from
    // those bundled into this JAR.
    conf.setJarByClass(HelloWorld.class);

    // Runs the job.
    JobClient.runJob(conf);
  }
}
