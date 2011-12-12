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
//import org.jsoup.Jsoup;

public class WordCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
	  //try {
		// Un-GZIP compressed page data
		ByteArrayInputStream inputStream = new ByteArrayInputStream(
            value.getContent().getReadOnlyBytes(), 0, value.getContent().getCount());
	    String page_content = new Scanner(inputStream).useDelimiter("\\A").next();
		/*BufferedReader reader = new BufferedReader(
            new InputStreamReader(inputStream,Charset.forName("ASCII")));
		String page_content = "";*/
		
		//while ((page_content += reader.readLine()) != null) {}
		//GZIPInputStream gzipIn = new GZIPInputStream(value.getContent().getBytes());
	    
	    //String page_content = new Scanner(inputStream).useDelimiter("\\A").next();
        //String page_content = Jsoup.parse(value.getContent().toString()).text();
  	    // Remove all punctuation
	    page_content.replaceAll("\\p{Punct}", "");
	    // Normalize whitespace to single spaces
	    page_content.replaceAll("\\t|\\n", " ");
	    page_content.replaceAll("\\s+", " ");
	    // Split by space and output to OutputCollector
	    for (String word: page_content.split(" ")) {
		  output.collect(new Text(word), new LongWritable(1));
	    }
	  //} catch (IOException e) {
      //  return;  // not in GZIP format, we can't process it
	  //}
  }
}
