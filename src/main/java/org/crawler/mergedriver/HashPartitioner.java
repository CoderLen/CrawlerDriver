package org.crawler.mergedriver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.crawler.util.DocumentWritable;

public class HashPartitioner extends Partitioner<Text, DocumentWritable> {

	@Override
	public int getPartition(Text key, DocumentWritable value, int numReduceTasks) {
		String url = key.toString();
		if(url.contains("http://")){
			url = url.replace("http://", "");
		}
		if(url.contains("/")){
			url = url.substring(0, url.indexOf("/"));
		}
		return (url.hashCode() & Integer.MAX_VALUE) % numReduceTasks;  
	}

}
