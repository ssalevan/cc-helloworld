package org.commoncrawl.tutorial;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.jsoup.Jsoup;

public class WordCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
	// Un-GZIP compressed page data
	ByteArrayInputStream inputStream = new ByteArrayInputStream(
        value.getContent().getReadOnlyBytes(), 0,
        value.getContent().getCount());
    String content = new Scanner(inputStream)
        .useDelimiter("\\A").next();
    String page_text = Jsoup.parse(value.getContent().toString()).text();
    // Remove all punctuation
    page_text.replaceAll("\\p{Punct}", "");
    // Normalize whitespace to single spaces
    page_text.replaceAll("\\t|\\n", " ");
    page_text.replaceAll("\\s+", " ");
    // Split by space and output to OutputCollector
    for (String word: page_text.split(" ")) {
      output.collect(new Text(word), new LongWritable(1));
    }
  }
}
