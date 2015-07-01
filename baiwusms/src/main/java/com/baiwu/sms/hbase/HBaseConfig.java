package com.baiwu.sms.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

 /**
 * @author chenbaoyu
 *
 */
public class HBaseConfig {

	public static Configuration cfg = null;
	private static final Log logger = LogFactory.getLog(HBaseConfig.class);
	static {
		try {
			load("hbase-site.xml");
		} catch (FileNotFoundException e) {
			logger.error(HBaseConfig.class,e);
		} catch (IOException e) {
			logger.error(HBaseConfig.class,e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.pzoom.ads.report.config.Config#load(java.lang.String)
	 */
	public static void load(String path) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		if (cfg == null) {
			cfg = HBaseConfiguration.create();
			cfg.addResource(path);
		}
	}

	public static Configuration getConfiguration() {
		return cfg;
	}
}

