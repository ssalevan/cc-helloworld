package org.commoncrawl.tutorial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.jsoup.Jsoup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Outputs all words contained within the displayed text of pages contained
 * within {@code ArcFileItem} objects.
 * 
 * @author Steve Salevan <steve.salevan@gmail.com>
 */
public class WordCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {
  private final static Pattern REGEX_NON_ALPHANUMERIC = Pattern.compile("[^a-zA-Z0-9 ]");
  private final static Pattern REGEX_SPACE = Pattern.compile("\\s+");

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
    try {
      if (!value.getMimeType().contains("text")) {
        return;  // Only parse text.
      }
      // Retrieves page content from the passed-in ArcFileItem.
      ByteArrayInputStream inputStream = new ByteArrayInputStream(
          value.getContent().getReadOnlyBytes(), 0,
          value.getContent().getCount());
      // Converts InputStream to a String.
      String content = new Scanner(inputStream).useDelimiter("\\A").next();
      // Parses HTML with a tolerant parser and extracts all text.
      String pageText = Jsoup.parse(content).text();
      // Removes all punctuation.
      pageText = REGEX_NON_ALPHANUMERIC.matcher(pageText).replaceAll("");
      // Splits by space and outputs to OutputCollector.
      for (String word : REGEX_SPACE.split(pageText)) {
        output.collect(new Text(word), new LongWritable(1));
      }
    }
    catch (Exception e) {
      reporter.getCounter("WordCountMapper.exception",
          e.getClass().getSimpleName()).increment(1);
    }
  }
}
