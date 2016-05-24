package org.crawler.htmltoxmldriver;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.crawler.mergedriver.HashPartitioner;
import org.crawler.parserdriver.ParserInputFormat;
import org.crawler.util.DocumentWritable;

public class HtmlToXMLDriver {
	
	public static class HtmlToXMLMapper extends Mapper<Text,DocumentWritable,Text,Text>{
		
		public void map(Text key,DocumentWritable value,Context context){
			
		}
		
		public static void htmltoxml(String html){
			String regex = "<title>.*?</title>";
			Pattern pattern = Pattern.compile(regex);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HtmlToXMLDriver");
		job.setJarByClass(HtmlToXMLDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(Mapper.class);
		// TODO: specify a reducer 
		job.setReducerClass(Reducer.class);

		job.setInputFormatClass(ParserInputFormat.class);
		job.setPartitionerClass(HashPartitioner.class);
		
		job.setNumReduceTasks(3);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DocumentWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/doc"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/doc"));

		if (!job.waitForCompletion(true))
			return;
	}
}
