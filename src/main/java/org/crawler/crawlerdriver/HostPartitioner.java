package org.crawler.crawlerdriver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//�ӿ�Partitioner�̳�JobConfigurable����������������override����
public class HostPartitioner extends Partitioner<Text, LongWritable>{

	/** 
     * getPartition()������ 
     * �����������/ֵ��<key,value>��reducer����numReduceTasks 
     * ��������������Reducer��ţ�������result 
     * */  
	@Override
	public int getPartition(Text key, LongWritable value, int numReduceTasks) {
		 // TODO Auto-generated method stub
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
