package org.crawler.optimizerdriver;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.crawler.util.OutLinksWritable;
import org.crawler.util.TextArrayWritable;

public class OptimizerDriver {

	public static class OptimizerMapper extends
			Mapper<Text, OutLinksWritable, Text, BooleanWritable> {

		public void map(Text key, OutLinksWritable value, Context context)
				throws IOException, InterruptedException {
			BooleanWritable trueflag = new BooleanWritable(true);
			BooleanWritable falseflag = new BooleanWritable(false);

			context.write(key, trueflag);
			System.out.println("url:"+key+",flag:"+trueflag.toString());
			TextArrayWritable urlarray = value.getOutLinks();
			String urls[] = urlarray.toStrings();
			for (String url : urls) {
				Text urltext = new Text(url);
				System.out.println("url:"+url+",flag:"+falseflag.toString());
				context.write(urltext, falseflag);
			}
		}
	}

	public static class OptimizerReducer extends
			Reducer<Text, BooleanWritable, Text, Text> {

		public void reduce(Text key, Iterable<BooleanWritable> value,
				Context context) throws IOException, InterruptedException {
	
			Iterator<BooleanWritable> v = value.iterator();
			boolean flag = false;
			while (v.hasNext()) {
				BooleanWritable booleanWritable = v.next();
				if (booleanWritable.get()  == true) {
//					System.out.println("value:"+booleanWritable.get());
					flag = true;
					break;
				}
			}
			if(flag ==  false){
				System.out.println("key"+key);
				context.write(new Text(""), key);
			}else{
				System.out.printf("the %s has been doon! \n ",key);
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path outPath = new Path("/in");
		FileSystem.get(conf).delete(outPath, true);

		Job job = Job.getInstance(conf, "OptimizerDriver");
		job.setJarByClass(OptimizerDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(OptimizerMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(OptimizerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		job.setNumReduceTasks(3);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(OptimizerPartitioner.class);
		job.setInputFormatClass(OptimizerInputFormat.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/output/4"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/url/5"));

		if (!job.waitForCompletion(true))
			return;
	}

}
