package org.crawler.crawlerdriver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.crawler.util.DocumentWritable;
import org.crawler.util.Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlerDriver {
	
	public static Logger logger = LoggerFactory.getLogger(CrawlerDriver.class);
	
	public static class InverseMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable ikey, Text ivalue, Context context)
				throws IOException, InterruptedException {
			logger.info("InverseMapper End!");
			context.write(ivalue, ikey);
		}

	}
	
	public static class CrawlerReducer extends Reducer<Text,LongWritable,Text,DocumentWritable>{
		//private DocumentWritable documentWritable = new DocumentWritable();
		public void reduce(Text _key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			logger.info("Start CrawlerReducer...");
			logger.info("url:"+_key);
			String document = Downloader.Download(_key);
			DocumentWritable documentWritable = new DocumentWritable(_key.toString(),document);
			logger.info("documentWritable: "+documentWritable.toString());
			context.write(_key, documentWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
	    Path outPath = new Path("/doc");
        FileSystem.get(conf).delete(outPath, true);
        
		Job job = Job.getInstance(conf, "CrawlerDriver");
		job.setJarByClass(CrawlerDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(InverseMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(CrawlerReducer.class);

		job.setNumReduceTasks(3);
		job.setPartitionerClass(HostPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, new Path("hdfs://bigdata01:8020/in"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://bigdata01:8020/doc/5"));

		if (!job.waitForCompletion(true))
			return;
	}

}
