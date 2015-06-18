package org.crawler.parserdriver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.crawler.util.DocumentWritable;

public class ParserInputFormat extends FileInputFormat<Text, DocumentWritable> {
	
    @Override
    public RecordReader<Text, DocumentWritable> createRecordReader( 
            InputSplit split, TaskAttemptContext context) { 
        return new ParserRecordReader(); 
    } 
}
