package com.baiwu.sms;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.baiwu.sms.hbase.HBaseConfig;

public class BaiWuSms {
	
	private static Log logger = LogFactory.getLog(BaiWuSms.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// if(args[0].equals("start"))
		// {
		//
		// }else if(args[0].equals("startlocal"))
		// {
		// startLocal();
		// }

		start(args);
	}

	public static void start(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		long start = System.currentTimeMillis();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: baiwusms <in> <out>");
			System.exit(2);
		}
		try {
			DefaultStringifier.store(conf, HBaseConfig.getConfiguration() ,"hbaseconfiguration");
		} catch (IOException e) {
			logger.error("", e);
		}
		
		Job job = new Job(conf, "baiwusms");

		job.setJarByClass(BaiWuSms.class);
		job.setMapperClass(SmsMapper.class);
		job.setReducerClass(SmsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SmsInputFormat.class);
		job.setOutputFormatClass(SmsOutputFormat.class);

		job.setNumReduceTasks(5);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		long end = System.currentTimeMillis();
		long inteval = start-end;
		
		logger.info("mapred 耗时："+inteval+" 毫秒");
		System.exit(exitFlag);
	}

	public static void startLocal() throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "baiwusms");

		job.setJarByClass(BaiWuSms.class);
		job.setMapperClass(SmsMapper.class);
		job.setReducerClass(SmsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayList.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SmsInputFormat.class);
		job.setOutputFormatClass(SmsOutputFormat.class);

		job.setNumReduceTasks(5);

		FileInputFormat.addInputPath(job, new Path("e:\\export"));
		FileOutputFormat.setOutputPath(job, new Path("e:\\export\\output"));

		int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitFlag);
	}
	
	
}