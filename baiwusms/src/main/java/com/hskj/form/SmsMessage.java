package com.hskj.form;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;

public class SmsMessage implements Serializable, WritableComparable<SmsMessage>{
	private static final long serialVersionUID = 4636185589756757776L;
	private int submit_sn;
	private String user_id;
	private int user_sn;
	private String src_number;
	private String sp_number;
	private int yw_code;
	private String mobile;
	private String msg_content;
	private String msg_id;
	private String insert_time;
	private String update_time;
	private int status;
	private int response;
	private String fail_desc;
	private String tmp_msg_id;
	private int stat_flag;
	private int pknumber;
	private int pktotal;
	private int sub_msg_id;
	private String complete_content;
	private int msg_format;
	private int charge_count;
	private int pool_id;
	private String msg_receiveTime;
	private String msg_wordCheckTime;
	private String msg_scanTime;
	private String msg_sendTime;
	private String msg_reportTime;
	private String dest_flag;
	private String user_name;
	private int long_msg_id;
	private int long_msg_count;
	private int long_msg_sub_sn;
	private double price;
	private String signature;
	private String country_cn;
	private String ori_mobile;
	private int sn;
	private int report_sn;
	private int try_times;
	private String arr_time;
	private String sub_sn;
	private String err;
	private int stat;
	private Map<String, Object> extraFields = new HashMap();

	public String getReportMatchKey() {
		return this.tmp_msg_id + "_" + this.mobile;
	}

	@Override
	public String toString() {
		return "SmsMessage [submit_sn=" + submit_sn + ", user_id=" + user_id
				+ ", user_sn=" + user_sn + ", src_number=" + src_number
				+ ", sp_number=" + sp_number + ", yw_code=" + yw_code
				+ ", mobile=" + mobile + ", msg_content=" + msg_content
				+ ", msg_id=" + msg_id + ", insert_time=" + insert_time
				+ ", update_time=" + update_time + ", status=" + status
				+ ", response=" + response + ", fail_desc=" + fail_desc
				+ ", tmp_msg_id=" + tmp_msg_id + ", stat_flag=" + stat_flag
				+ ", pknumber=" + pknumber + ", pktotal=" + pktotal
				+ ", sub_msg_id=" + sub_msg_id + ", complete_content="
				+ complete_content + ", msg_format=" + msg_format
				+ ", charge_count=" + charge_count + ", pool_id=" + pool_id
				+ ", msg_receiveTime=" + msg_receiveTime
				+ ", msg_wordCheckTime=" + msg_wordCheckTime
				+ ", msg_scanTime=" + msg_scanTime + ", msg_sendTime="
				+ msg_sendTime + ", msg_reportTime=" + msg_reportTime
				+ ", dest_flag=" + dest_flag + ", user_name=" + user_name
				+ ", long_msg_id=" + long_msg_id + ", long_msg_count="
				+ long_msg_count + ", long_msg_sub_sn=" + long_msg_sub_sn
				+ ", price=" + price + ", signature=" + signature
				+ ", country_cn=" + country_cn + ", ori_mobile=" + ori_mobile
				+ ", sn=" + sn + ", report_sn=" + report_sn + ", try_times="
				+ try_times + ", arr_time=" + arr_time + ", sub_sn=" + sub_sn
				+ ", err=" + err + ", stat=" + stat + ", extraFields="
				+ extraFields + "]";
	}

	public void addExtraField(String key, Object value) {
		boolean isKeyOK = (key != null) && (!key.equals(""));
		boolean isValueOK = value != null;
		if ((isKeyOK) && (isValueOK)) {
			if (this.extraFields == null) {
				this.extraFields = new HashMap();
			}
			this.extraFields.put(key, value);
		} else {
			System.out.println("��������setʧ�ܣ�isKeyOK = " + key
					+ " && isValueOK = " + value);
		}
	}

	public Object getExtraField(String key) {
		Object result = null;
		boolean isExtraNotNull = this.extraFields != null;
		boolean isKeyOK = (key != null) && (!key.equals(""));
		if ((isExtraNotNull) && (isKeyOK)) {
			result = this.extraFields.get(key);
		}
		return result;
	}

	public Object clone() {
		SmsMessage result = null;
		try {
			result = (SmsMessage) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return result;
	}

	public String getCountry_cn() {
		return this.country_cn;
	}

	public void setCountry_cn(String country_cn) {
		this.country_cn = country_cn;
	}

	public String getOri_mobile() {
		return this.ori_mobile;
	}

	public void setOri_mobile(String ori_mobile) {
		this.ori_mobile = ori_mobile;
	}

	public String getUser_id() {
		return this.user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public int getSn() {
		return this.sn;
	}

	public void setSn(int sn) {
		this.sn = sn;
	}

	public int getSubmit_sn() {
		return this.submit_sn;
	}

	public void setSubmit_sn(int submit_sn) {
		this.submit_sn = submit_sn;
	}

	public int getReport_sn() {
		return this.report_sn;
	}

	public void setReport_sn(int report_sn) {
		this.report_sn = report_sn;
	}

	public int getTry_times() {
		return this.try_times;
	}

	public void setTry_times(int try_times) {
		this.try_times = try_times;
	}

	public String getArr_time() {
		return this.arr_time;
	}

	public void setArr_time(String arr_time) {
		this.arr_time = arr_time;
	}

	public String getSub_sn() {
		return this.sub_sn;
	}

	public void setSub_sn(String sub_sn) {
		this.sub_sn = sub_sn;
	}

	public int getStat() {
		return this.stat;
	}

	public void setStat(int stat) {
		this.stat = stat;
	}

	public int getLong_msg_id() {
		return this.long_msg_id;
	}

	public void setLong_msg_id(int long_msg_id) {
		this.long_msg_id = long_msg_id;
	}

	public int getLong_msg_count() {
		return this.long_msg_count;
	}

	public void setLong_msg_count(int long_msg_count) {
		this.long_msg_count = long_msg_count;
	}

	public int getLong_msg_sub_sn() {
		return this.long_msg_sub_sn;
	}

	public void setLong_msg_sub_sn(int long_msg_sub_sn) {
		this.long_msg_sub_sn = long_msg_sub_sn;
	}

	public String getUser_name() {
		return this.user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	public String getDest_flag() {
		return this.dest_flag;
	}

	public void setDest_flag(String dest_flag) {
		if (dest_flag == null)
			dest_flag = "";
		else
			this.dest_flag = dest_flag;
	}

	public int getUser_sn() {
		return this.user_sn;
	}

	public void setUser_sn(int userSn) {
		this.user_sn = userSn;
	}

	public int getYw_code() {
		return this.yw_code;
	}

	public void setYw_code(int ywCode) {
		this.yw_code = ywCode;
	}

	public String getMobile() {
		return this.mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getSp_number() {
		return this.sp_number;
	}

	public void setSp_number(String spNumber) {
		this.sp_number = spNumber;
	}

	public String getMsg_id() {
		return this.msg_id;
	}

	public void setMsg_id(String msgId) {
		this.msg_id = msgId;
	}

	public String getMsg_content() {
		return this.msg_content;
	}

	public void setMsg_content(String msgContent) {
		this.msg_content = msgContent;
	}

	public String getInsert_time() {
		return this.insert_time;
	}

	public void setInsert_time(String insertTime) {
		this.insert_time = insertTime;
	}

	public String getUpdate_time() {
		return this.update_time;
	}

	public void setUpdate_time(String updateTime) {
		this.update_time = updateTime;
	}

	public int getStatus() {
		return this.status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getResponse() {
		return this.response;
	}

	public void setResponse(int response) {
		this.response = response;
	}

	public int getPknumber() {
		return this.pknumber;
	}

	public void setPknumber(int pknumber) {
		this.pknumber = pknumber;
	}

	public int getPktotal() {
		return this.pktotal;
	}

	public void setPktotal(int pktotal) {
		this.pktotal = pktotal;
	}

	public int getSub_msg_id() {
		return this.sub_msg_id;
	}

	public void setSub_msg_id(int subMsgId) {
		this.sub_msg_id = subMsgId;
	}

	public int getMsg_format() {
		return this.msg_format;
	}

	public String getMsg_receiveTime() {
		return this.msg_receiveTime;
	}

	public void setMsg_receiveTime(String msg_receiveTime) {
		this.msg_receiveTime = msg_receiveTime;
	}

	public String getMsg_scanTime() {
		return this.msg_scanTime;
	}

	public void setMsg_scanTime(String msg_scanTime) {
		this.msg_scanTime = msg_scanTime;
	}

	public String getMsg_wordCheckTime() {
		return this.msg_wordCheckTime;
	}

	public void setMsg_wordCheckTime(String msg_wordCheckTime) {
		this.msg_wordCheckTime = msg_wordCheckTime;
	}

	public String getMsg_sendTime() {
		return this.msg_sendTime;
	}

	public void setMsg_sendTime(String msg_sendTime) {
		this.msg_sendTime = msg_sendTime;
	}

	public String getMsg_reportTime() {
		return this.msg_reportTime;
	}

	public void setMsg_reportTime(String msg_reportTime) {
		this.msg_reportTime = msg_reportTime;
	}

	public void setMsg_format(int msgFormat) {
		this.msg_format = msgFormat;
	}

	public String getSrc_number() {
		return this.src_number;
	}

	public void setSrc_number(String srcNumber) {
		this.src_number = srcNumber;
	}

	public int getCharge_count() {
		return this.charge_count;
	}

	public void setCharge_count(int chargeCount) {
		this.charge_count = chargeCount;
	}

	public int getPool_id() {
		return this.pool_id;
	}

	public void setPool_id(int pool_id) {
		this.pool_id = pool_id;
	}

	public void setFail_desc(String fail_desc) {
		this.fail_desc = fail_desc;
	}

	public String getFail_desc() {
		if (this.fail_desc == null) {
			this.fail_desc = "";
		}
		return this.fail_desc;
	}

	public void setTmp_msg_id(String tmp_msg_id) {
		this.tmp_msg_id = tmp_msg_id;
	}

	public String getTmp_msg_id() {
		return this.tmp_msg_id;
	}

	public void setStat_flag(int stat_flag) {
		this.stat_flag = stat_flag;
	}

	public int getStat_flag() {
		return this.stat_flag;
	}

	public void setErr(String err) {
		if (err == null)
			err = "";
		else
			this.err = err;
	}

	public String getErr() {
		return this.err;
	}

	public void setComplete_content(String complete_content) {
		this.complete_content = complete_content;
	}

	public String getComplete_content() {
		return this.complete_content;
	}

	public void setExtraFields(Map<String, Object> extraFields) {
		this.extraFields = extraFields;
	}

	public Map<String, Object> getExtraFields() {
		return this.extraFields;
	}

	public double getPrice() {
		return this.price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public String getSignature() {
		return this.signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(SmsMessage o) {
		// TODO Auto-generated method stub
		return 0;
	}
}