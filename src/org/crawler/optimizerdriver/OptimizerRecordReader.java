package org.crawler.optimizerdriver;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.crawler.util.OutLinksWritable;
import org.crawler.util.TextArrayWritable;

public class OptimizerRecordReader extends RecordReader<Text,OutLinksWritable>{

	private LineReader in;
	private boolean more = false;
	
	private Text key = null;
	private OutLinksWritable value = null;
	
	private long start;
	private long pos;
	private long end;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(null != in){
			in.close();
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public OutLinksWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		if(false == more || end == start){
			return 0f;
		}else{
			return Math.min(1.0f, (pos - start)/(end - start));
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fileSplit = (FileSplit)split;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());
		
		fileIn.seek(start);
		
		in = new LineReader(fileIn,context.getConfiguration());
		if(start != 0){
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
	}

	private int maxBytesToConsume(long pos){
		return (int)Math.min(Integer.MAX_VALUE, end - pos);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(null == key){
			key = new Text();
		}
		if(null == value){
			value = new OutLinksWritable();
		}
		
		Text nowline = new Text();
		int readsize = in.readLine(nowline);
		//���µ�ǰ��ȡ����λ��
		pos += readsize;
		//���posֵ���ڵ���end��˵���˷�Ƭ�Ѿ���ȡ���
		if(pos >= end){
			more = false;
			return false;
		}
		
		if(0 == readsize){
			key = null;
			value = null;
			more = false;//�˴�˵���Ѿ���ȡ���ļ�ĩβ
			return false;
		}
		
		String[] keyandvalue = nowline.toString().split("\t");
		//�ų���һ��
		if(keyandvalue[0].endsWith("\"CITING\"")){
			readsize = in.readLine(nowline);
			//���µ�ǰ��ȡ��λ��
			pos += readsize;
			if(0 == readsize){
				more = false;
				return false;
			}
			//���»���
			keyandvalue = nowline.toString().split(",");
		}
		
		key.set(keyandvalue[0]);
		String[] values = keyandvalue[1].toString().split("\\|");
		
		if(values.length > 3){
			Text[] urltext = new Text[values.length - 2];
			for(int i=0;i<values.length - 2;i++){
				System.out.println("url:"+values[i]);
				urltext[i] = new Text(values[i]);
			}
			LongWritable timestramp = new LongWritable(Long.parseLong(values[values.length-1]));
			IntWritable typeOfOutLink = new IntWritable(Integer.parseInt(values[values.length-2]));
			//timestramp.set(Long.parseLong(values[values.length-2]));
			TextArrayWritable urls = new TextArrayWritable(urltext);
			value.setOutLinks(urls);
			value.setTimeStamp(timestramp);
			value.setTypeOfOutlink(typeOfOutLink);
		}
		
		return true;
	}

}
