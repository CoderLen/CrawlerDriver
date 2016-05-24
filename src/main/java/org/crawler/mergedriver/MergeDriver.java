package org.crawler.mergedriver;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.crawler.parserdriver.ParserInputFormat;
import org.crawler.util.DocumentWritable;

public class MergeDriver {

	public static class IdentityMapper extends Mapper<Text,DocumentWritable,Text,DocumentWritable>{
		
		public void map(Text key,DocumentWritable value,Context context) throws IOException, InterruptedException{
			context.write(key, value);
		}
	}
	
	public static class MergeDocReducer extends Reducer<Text,DocumentWritable,Text,DocumentWritable>{
		
		public void reduce(Text key,Iterable<DocumentWritable> value,Context context) throws IOException, InterruptedException{
			Iterator<DocumentWritable> temp = value.iterator();
			if(temp.hasNext()){
				DocumentWritable doc = temp.next();
				context.write(key, doc);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MergeDriver");
		job.setJarByClass(MergeDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(IdentityMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(MergeDocReducer.class);

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
