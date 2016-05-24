package org.crawler.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class OutLinksWritable implements WritableComparable<OutLinksWritable> {

	/**
	 * һ����� url �����顣
	 * ���δ�����ض����� outlinksΪ����ҳ�н����������������Ӽ��ϣ�
	 * ����,outlinksֻ��һ��Ԫ�أ�Ϊ�ض�������ʵ url
	 */
	private TextArrayWritable outLinks = new TextArrayWritable();
	
	/**
	 * ��� outlinks ������� url ���͡�
	 * ������������Ӽ����� typeOfOutlink Ϊ0��
	 * �������ʵ url �� typeOfOutlink Ϊ1��
	 */
	private IntWritable typeOfOutlink;
	
	/**
	 * ÿ������ URL ����д��һ�� OutLinksWritable ���͵�����ʱ
	 * ��Ϊ�����������һ��ʱ������������ֲ�ͬ������
	 */
	private LongWritable timeStamp;
	

	public TextArrayWritable getOutLinks() {
		return outLinks;
	}

	public void setOutLinks(TextArrayWritable outLinks) {
		this.outLinks = outLinks;
	}

	public IntWritable getTypeOfOutlink() {
		return typeOfOutlink;
	}

	public void setTypeOfOutlink(IntWritable typeOfOutlink) {
		this.typeOfOutlink = typeOfOutlink;
	}

	public LongWritable getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(LongWritable timeStamp) {
		this.timeStamp = timeStamp;
	}

	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.outLinks.readFields(input);
		this.timeStamp.readFields(input);
		this.typeOfOutlink.readFields(input);
	}

	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		this.outLinks.write(output);
		this.timeStamp.write(output);
		this.typeOfOutlink.write(output);
	}

	public int compareTo(OutLinksWritable arg0) {
		// TODO Auto-generated method stub
		int cmp = this.outLinks.toString().compareTo(arg0.outLinks.toString());
		if(cmp != 0){
			return cmp;
		}
		int cmp2 = this.timeStamp.compareTo(arg0.timeStamp);
		if(cmp != 0){
			return cmp2;
		}
		return this.typeOfOutlink.compareTo(arg0.typeOfOutlink);
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		String[] urls = this.outLinks.toStrings();
		int i = 1;
		for(String t : urls){
			
			System.out.println("url"+i+": "+t);
			str.append(t).append("|");
			i++;
		}
		str.append(this.typeOfOutlink+"|"+this.timeStamp);
		return str.toString();
	}
	

}
