package com.baiwu.sms.jobconf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public abstract class AbstractJob extends ToolRunner {
	static Logger log = Logger.getLogger(AbstractJob.class);

	public Configuration getConfig() {
		Configuration hadoopConf = null;
		try {
			hadoopConf = new Configuration();
			hadoopConf.addResource("core-site.xml");
			String isdebug = hadoopConf.get("local.debug");

			if (StringUtils.isNotBlank(isdebug)) {
				boolean debug = Boolean.valueOf(isdebug);
				if (!debug) {
					String hadoopPort = hadoopConf.get("mapred.job.tracker.temp");
					if (StringUtils.isNotBlank(hadoopPort)) {
						hadoopConf.set("mapred.job.tracker", hadoopPort);
					}
					setHotRelease(hadoopConf);
					setCompress(hadoopConf);
				}
			}

		} catch (Exception e) {
			log.error(this,e);
		}
		return hadoopConf;
	}

	/**
	 * 热部署
	 * 
	 * @param confIn
	 */
	public static void setHotRelease(Configuration confIn) {
		String runJarPath = confIn.get("hot.chache.jar");
		String dependJarPath = confIn.get("hot.cache.path");
		if (StringUtils.isNotBlank(dependJarPath)) {
			// String[] jars = dependJar.split(",");
			StringBuffer jarFiles = new StringBuffer();
			StringBuffer cacheFiles = new StringBuffer();
			String fs = confIn.get("fs.defaultFS");
			if (StringUtils.isBlank(fs)) {
				fs = confIn.get("fs.default.name");
			}
			String classpath = confIn.get("mapred.job.classpath.files");
			String cachefile = confIn.get("mapred.cache.files");
			if (StringUtils.isNotBlank(classpath)) {
				jarFiles.append(classpath);
			}
			if (StringUtils.isNotBlank(cachefile)) {
				cacheFiles.append(cachefile);
			}
			jarFiles.append(new Path(runJarPath).toUri().getPath());
			cacheFiles.append(fs).append(runJarPath);

			try {
				FileSystem fileSys = FileSystem.get(confIn);
				FileStatus[] dependFiles = fileSys.listStatus(new Path(dependJarPath));
				for (FileStatus fileStatus : dependFiles) {
					String dependJarAllPath = dependJarPath + fileStatus.getPath().getName().toString();
					// System.out.println(fileStatus.getPath().getName().toString());
					jarFiles.append(":").append(new Path(dependJarAllPath).toUri().getPath());
					cacheFiles.append(",").append(fs).append(dependJarAllPath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			confIn.set("mapred.job.classpath.files", jarFiles.toString());
			confIn.set("mapred.cache.files", cacheFiles.toString());
		}
	}
	
	public void setCompress(Configuration conf) {
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, DefaultCodec.class);
	}

}
