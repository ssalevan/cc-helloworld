package org.commoncrawl.tutorial;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReducer extends MapReduceBase
  implements Reducer<Text, LongWritable, Text, LongWritable> {
  public void reduce(Text key, Iterator<LongWritable> values,
	      OutputCollector<Text, LongWritable> output, Reporter reporter)
	      throws IOException {
	long sum = 0;
	// Summates all word counts for this word.
	while (values.hasNext()) {
	  sum += values.next().get();
	}
	output.collect(key, new LongWritable(sum));
  }
}
