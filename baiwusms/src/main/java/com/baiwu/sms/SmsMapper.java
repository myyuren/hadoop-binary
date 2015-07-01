package com.baiwu.sms;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hskj.form.SmsMessage;

public class SmsMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

	private Text file = new Text();

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		String fileName = context.getConfiguration().get("map.input.file");
		file.set(fileName);
		context.write(file,value);
	}
}
