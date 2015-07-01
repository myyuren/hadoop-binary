package com.baiwu.sms;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hskj.form.SmsMessage;

public class SmsReducer extends Reducer<Text, BytesWritable, Text, Text> {
	private static Log LOG = LogFactory.getLog(SmsReducer.class);
	
	private static Configuration conf = null;
	
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
	
		conf = DefaultStringifier.load(context.getConfiguration(), "hbaseconfiguration", Configuration.class );
		InputStream in = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int i = 0;
		HTable table = new HTable(conf, "BaiWuSms6");
		List<Put> lp = new ArrayList<Put>();
		for (BytesWritable value : values) {
			in = new ByteArrayInputStream(value.getBytes());
			List<SmsMessage> smsList = FileUtilSeri.readObjectFromFile(in);
			for (SmsMessage sms : smsList) {
				  long msg_receive_time = 0;
				  try {
					  msg_receive_time = sdf.parse(sms.getMsg_receiveTime()).getTime();
				  } catch (ParseException e) {
					  e.printStackTrace();
				  }

				  StringBuffer rowKey = new StringBuffer(sms.getExtraField(
						"server_num").toString());
				  rowKey.append(sms.getUser_sn()).append(msg_receive_time)
						.append(sms.getSn());
				  byte[] rowKeyBytes = rowKey.toString().getBytes();
				  Put put = new Put(rowKeyBytes);
				  if(StringUtils.isNotEmpty(sms.getSub_sn()))
				  {
					  put.add("sms".getBytes(), "submit_sn".getBytes(), TypeConvert.intToByteArray(sms.getSubmit_sn()));
				  }
				  if(StringUtils.isNotEmpty(sms.getUser_id()))
				  {
					  put.add("sms".getBytes(), "user_id".getBytes(),sms.getUser_id().getBytes());
				  }
				 
		          put.add("sms".getBytes(), "user_sn".getBytes(), TypeConvert.intToByteArray(sms.getUser_sn()));
		          if(StringUtils.isNotEmpty(sms.getSrc_number()))
				  {
		        	  put.add("sms".getBytes(), "src_number".getBytes(), sms.getSrc_number().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getSp_number()))
				  {
		        	  put.add("sms".getBytes(), "sp_number".getBytes(),sms.getSp_number().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "yw_code".getBytes(), TypeConvert.intToByteArray(sms.getYw_code()));
		          if(StringUtils.isNotEmpty(sms.getMobile()))
				  {
		        	  put.add("sms".getBytes(), "mobile".getBytes(), sms.getMobile().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getMsg_content()))
				  {
		        	  put.add("sms".getBytes(), "msg_content".getBytes(), sms.getMsg_content().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getMsg_id()))
				  {
		        	  put.add("sms".getBytes(), "msg_id".getBytes(), sms.getMsg_id().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getInsert_time()))
				  {
		        	  put.add("sms".getBytes(), "insert_time".getBytes(),sms.getInsert_time().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getUpdate_time()))
				  {
		        	  put.add("sms".getBytes(), "update_time".getBytes(),sms.getUpdate_time().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "status".getBytes(), TypeConvert.intToByteArray(sms.getStatus()));
		          put.add("sms".getBytes(), "response".getBytes(), TypeConvert.intToByteArray(sms.getResponse()));
		          if(StringUtils.isNotEmpty(sms.getFail_desc()))
				  {
		        	  put.add("sms".getBytes(), "fail_desc".getBytes(),sms.getFail_desc().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getTmp_msg_id()))
				  {
		        	  put.add("sms".getBytes(), "tmp_msg_id".getBytes(),sms.getTmp_msg_id().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "stat_flag".getBytes(), TypeConvert.intToByteArray(sms.getStat_flag()));
		          put.add("sms".getBytes(), "pknumber".getBytes(), TypeConvert.intToByteArray(sms.getPknumber()));
		          put.add("sms".getBytes(), "pktotal".getBytes(), TypeConvert.intToByteArray(sms.getPktotal()));
		          put.add("sms".getBytes(), "sub_msg_id".getBytes(), TypeConvert.intToByteArray(sms.getSub_msg_id()));
		          if(StringUtils.isNotEmpty(sms.getComplete_content()))
				  {
		        	  put.add("sms".getBytes(), "complete_content".getBytes(), sms.getComplete_content().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "msg_format".getBytes(), TypeConvert.intToByteArray(sms.getMsg_format()));
		          put.add("sms".getBytes(), "charge_count".getBytes(), TypeConvert.intToByteArray(sms.getCharge_count()));
		          put.add("sms".getBytes(), "pool_id".getBytes(), TypeConvert.intToByteArray(sms.getPool_id()));
		          if(StringUtils.isNotEmpty(sms.getMsg_receiveTime()))
				  {
		        	  put.add("sms".getBytes(), "msg_receiveTime".getBytes(),sms.getMsg_receiveTime().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getMsg_wordCheckTime()))
				  {
		        	  put.add("sms".getBytes(), "msg_wordCheckTime".getBytes(),sms.getMsg_wordCheckTime().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getMsg_scanTime()))
				  {
		        	  put.add("sms".getBytes(), "msg_scanTime".getBytes(),sms.getMsg_scanTime().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getMsg_reportTime()))
				  {
		        	  put.add("sms".getBytes(), "msg_reportTime".getBytes(),sms.getMsg_reportTime().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getDest_flag()))
				  {
		        	  put.add("sms".getBytes(), "dest_flag".getBytes(),sms.getDest_flag().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getUser_name()))
				  {
		        	  put.add("sms".getBytes(), "user_name".getBytes(),sms.getUser_name().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "long_msg_id".getBytes(), TypeConvert.intToByteArray(sms.getLong_msg_id()));
		          put.add("sms".getBytes(), "long_msg_count".getBytes(), TypeConvert.intToByteArray(sms.getLong_msg_count()));
		          put.add("sms".getBytes(), "long_msg_sub_sn".getBytes(), TypeConvert.intToByteArray(sms.getLong_msg_sub_sn()));
		          put.add("sms".getBytes(), "price".getBytes(), TypeConvert.doubleToByteArray(sms.getPrice(), 0));
		          if(StringUtils.isNotEmpty(sms.getSignature()))
				  {
		        	  put.add("sms".getBytes(), "signature".getBytes(),sms.getSignature().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getCountry_cn()))
				  {
		        	  put.add("sms".getBytes(), "country_cn".getBytes(), sms.getCountry_cn().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getOri_mobile()))
				  {
		        	  put.add("sms".getBytes(), "ori_mobile".getBytes(), sms.getOri_mobile().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "sn".getBytes(), TypeConvert.intToByteArray(sms.getSn()));
		          put.add("sms".getBytes(), "report_sn".getBytes(), TypeConvert.intToByteArray(sms.getReport_sn()));
		          put.add("sms".getBytes(), "try_times".getBytes(), TypeConvert.intToByteArray(sms.getTry_times()));
		          if(StringUtils.isNotEmpty(sms.getArr_time()))
				  {
		        	  put.add("sms".getBytes(), "arr_time".getBytes(),sms.getArr_time().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getSub_sn()))
				  {
		        	  put.add("sms".getBytes(), "sub_sn".getBytes(), sms.getSub_sn().getBytes());
				  }
		          if(StringUtils.isNotEmpty(sms.getErr()))
				  {
		        	  put.add("sms".getBytes(), "err".getBytes(),sms.getErr().getBytes());
				  }
		          
		          put.add("sms".getBytes(), "stat".getBytes(), TypeConvert.intToByteArray(sms.getStat()));
	
		          lp.add(put);
		          i++;
			}
			LOG.info(lp.size()+"---------------------------------"+i);
			table.put(lp);
			lp.clear();
			
		}
		table.close();
	}
}