package org.commoncrawl.tutorial;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.protocol.shared.ArcFileItem;

import org.jsoup.Jsoup;

/**
 * Outputs all words contained within the displayed text of pages contained
 * within {@code ArcFileItem} objects.
 * 
 * @author Steve Salevan <steve.salevan@gmail.com>
 */
public class WordCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
	// Retrieves page content from the passed-in ArcFileItem.
	ByteArrayInputStream inputStream = new ByteArrayInputStream(
        value.getContent().getReadOnlyBytes(), 0,
        value.getContent().getCount());
    String content = new Scanner(inputStream)
        .useDelimiter("\\A").next();
    // Parses HTML with a tolerant parser and extracts all text.
    String page_text = Jsoup.parse(content).text();
    // Removes all punctuation.
    page_text = page_text.replaceAll("\\p{Punct}", "");
    // Normalizes whitespace to single spaces.
    page_text = page_text.replaceAll("\\t|\\n", " ");
    page_text = page_text.replaceAll("\\s+", " ");
    // Splits by space and outputs to OutputCollector.
    for (String word: page_text.split(" ")) {
      output.collect(new Text(word), new LongWritable(1));
    }
  }
}
