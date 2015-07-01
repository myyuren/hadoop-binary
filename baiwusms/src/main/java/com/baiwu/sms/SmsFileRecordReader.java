package com.baiwu.sms;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SmsFileRecordReader extends RecordReader<Text, BytesWritable> {
	private static Logger LOG = LoggerFactory
			.getLogger(SmsFileRecordReader.class);
	private FileSplit fileSplit;
	private JobContext jobContext;
	private BytesWritable value;
	private Text currentKey = new Text();
	private boolean processed = false;

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {

		return this.value;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.jobContext = context;
		context.getConfiguration().set("map.input.file",
				fileSplit.getPath().getName());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			value = new BytesWritable();
			Path file = fileSplit.getPath();
			byte[] contents = new byte[(int) fileSplit.getLength()];
			FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				LOG.info(file.getName()+"**************************");
				this.currentKey = new Text(file.getName());
				// 将file文件中
				// 的内容放入contents数组中。使用了IOUtils实用类的readFully方法，将in流中得内容放入
				// contents字节数组中。
				IOUtils.readFully(in, contents, 0, contents.length);
				// BytesWritable是一个可用做key或value的字节序列，而ByteWritable是单个字节。
				// 将value的内容设置为contents的值
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	/*
	 * public static void main(String[] args) { FSDataInputStream hdfsInStream =
	 * fs.open(new Path("")); }
	 */

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}