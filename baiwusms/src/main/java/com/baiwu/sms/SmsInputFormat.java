package com.baiwu.sms;

import java.io.IOException;
import java.nio.file.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SmsInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordReader<Text, BytesWritable> recordReader = new SmsFileRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
	
	/** 
     * @brief isSplitable 不对文件进行切分，必须对文件整体进行处理 
     * 
     * @param fs 
     * @param file 
     * 
     * @return false 
     */  
    protected boolean isSplitable(FileSystem fs, Path file) {  
    //  CompressionCodec codec = compressionCodecs.getCode(file);   
        return false;//以文件为单位，每个单位作为一个split，即使单个文件的大小超过了64M，也就是H
    }
}
