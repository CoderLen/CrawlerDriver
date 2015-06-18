package org.crawler.parserdriver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.crawler.util.DocumentWritable;
import org.crawler.util.OutLinksWritable;
import org.crawler.util.Parser;
import org.crawler.util.TextArrayWritable;

public class ParserDriver {
	
	public static class ParserMapper extends Mapper<Text,DocumentWritable,Text,OutLinksWritable>{
		
		public ParserMapper(){
		}
		
		private TextArrayWritable textArrayWritable = new TextArrayWritable();
		private OutLinksWritable outLinksWritable = new OutLinksWritable();
		public void map(Text key,DocumentWritable value,Context context) throws IOException, InterruptedException{
//			System.out.println("key :"+key );
//			System.out.println("value :" + value.toString());
			Set<String> urls = Parser.parser(value.getDocument().toString());
//			System.out.println("Document :" + value.getDocument().toString());
//			for(String t : urls){
//				System.out.println("url :"+t);
//			}
			int typeOfOutlink = 0;
			if(urls.size() <= 1){
				typeOfOutlink = 1;
			}
			textArrayWritable = new TextArrayWritable(convert(urls));
			outLinksWritable.setOutLinks(textArrayWritable);
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhMMss");
			outLinksWritable.setTimeStamp(new LongWritable(Long.parseLong(format.format(new Date()))));
			outLinksWritable.setTypeOfOutlink(new IntWritable(typeOfOutlink));
//			System.out.println("getOutLinks :"+outLinksWritable.getOutLinks().toString());
			Text redirect = new Text();
			redirect.set(value.getRedirectFrom());
//			System.out.println("redirect :"+redirect);
//			System.out.println("outLinksWritable :" + outLinksWritable.toString());

			context.write(redirect, outLinksWritable);
		}
		
		/**
		 * @param tokens
		 * @return
		 */
		private Text[] convert(Set<String> tokens){
			Text[] t = new Text[tokens.size()];
			Iterator<String> it = tokens.iterator();
			int i = 0;
			while(it.hasNext()){
				t[i] = new Text(it.next());
				i++;
			}
			return t;
		}
	}
	
	public ParserDriver() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
	    Path outPath = new Path("/out");
        FileSystem.get(conf).delete(outPath, true);
        
		Job job = Job.getInstance(conf, "ParserDriver");
		job.setJarByClass(ParserDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(ParserMapper.class);
		// TODO: specify a reducer

		job.setNumReduceTasks(0);
		
		job.setPartitionerClass(ParserPartitioner.class);
		job.setInputFormatClass(ParserInputFormat.class);
		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutLinksWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/doc/2"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output/5"));

		if (!job.waitForCompletion(true))
			return;
	}
}
