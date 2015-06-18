package org.crawler.parserdriver;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.crawler.util.DocumentWritable;

public class ParserRecordReader extends RecordReader<Text, DocumentWritable> {
	
	//private static final Log LOG = LogFactory.getLog(ParserRecordReader.class); 
	
    private LineReader in; //������
    private boolean more = true; //��ʾ��������û������
    
    private Text key = null; 
    private DocumentWritable value = null;
    
    //�������Ǳ��浱ǰ��ȡ��λ�ã����ļ���λ�ã�
    private long start; 
    private long pos; 
    private long end; 
    
    /**
	 * ��ʼ������
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit inputSplit = (FileSplit)split;
		start = inputSplit.getStart();//�õ��˷�Ƭ�Ŀ�ͷλ��
		end = start + inputSplit.getLength();//�õ��˷�Ƭ�Ľ���λ��
		
		final Path file = inputSplit.getPath();
		//���ļ�
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		FSDataInputStream fileIn = fs.open(inputSplit.getPath());
		
		//���ļ�ָ���ƶ�����ǰ��Ƭ����Ϊÿ��Ĭ�ϴ��ļ�ʱ����ָ��ָ��ͷ
		fileIn.seek(start);
		
		in = new LineReader(fileIn,context.getConfiguration());
		if(start != 0){
			//����ⲻ�ǵ�һ����Ƭ����ô�����һ����Ƭ0-4����ô��4��λ�ñ���ȡ������Ҫ����4�������������������Ϊ���ͷ��ȥ��֮ǰ�����ĵط�
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		pos = start;
	}
	
	private int maxBytesToConsume(long pos){
		return (int)Math.min(Integer.MAX_VALUE, end - pos);
	}
	
	/**
	 * ��һ��ֵ
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(null == key){
			key = new Text();
		}
		if(null == value){
			value = new DocumentWritable();
		}
		Text nowline = new Text();//���浱ǰ������
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
		System.out.println("nowline:"+nowline.toString());
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
			keyandvalue = nowline.toString().split("\t");
		}
		System.out.println("LENGTH:" + keyandvalue.length);
		//�õ�key��value
		StringBuffer doc = new StringBuffer("");
		for(int i=1;i<keyandvalue.length-3;i++){
			doc.append(keyandvalue[i]);
		}
		System.out.println("DOC:" + doc.toString());
		key.set(keyandvalue[0]);
		value.setDocument(doc.toString());
		value.setRedirectFrom(keyandvalue[keyandvalue.length-3]);
		value.setMetaFollow(Boolean.getBoolean(keyandvalue[keyandvalue.length-2]));
		value.setMetaIndex(Boolean.getBoolean(keyandvalue[keyandvalue.length-1]));		
		return true;
	} 
   
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
	public DocumentWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(false == more || end == start){
			return 0f;
		}else{
			return Math.min(1.0f, (pos - start)/(end - start));
		}
	}
	
}
