package com.baiwu.sms;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.List;

import com.hskj.form.SmsMessage;

public class FileUtilSeri {
	public static List<SmsMessage> readObjectFromFile(InputStream file_name) {
		List<SmsMessage> list = null;
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(file_name);
			list = (List<SmsMessage>) ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				ois.close();
			} catch (Exception localException1) {
			}
		} finally {
			try {
				ois.close();
			} catch (Exception localException2) {

			}
		}
		return list;
	}
}
