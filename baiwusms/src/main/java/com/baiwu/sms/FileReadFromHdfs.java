package com.baiwu.sms;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hskj.form.SmsMessage;


public class FileReadFromHdfs {
	private static Logger LOG = LoggerFactory.getLogger(FileReadFromHdfs.class);
	public static void main(String[] args) {
		try {
		String dsf = "hdfs://Slave3.Hadoop:8020/output/bitest/1.sms";
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(dsf),conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dsf));
		
		List<SmsMessage> smsMessageList = FileUtilSeri.readObjectFromFile(hdfsInStream);
		System.out.println(smsMessageList.size()+"----------");
		LOG.info(smsMessageList.size()+"----------");
		hdfsInStream.close();
		fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

