
package com.baiwu.sms.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * @author chenbaoyu
 *
 */
public class HBaseClusterDao {
	private static Configuration conf = null;
	private static HTablePool pool;
	private static Log logger = LogFactory.getLog(HBaseClusterDao.class);

	public HBaseClusterDao(Configuration configuration) {
		super();
		HBaseClusterDao.conf = configuration;
		pool = new HTablePool(conf, 1024);// Integer.MAX_VALUE
	}


	public static Configuration getConf() {
		return conf;
	}


	public static void setConf(Configuration conf) {
		HBaseClusterDao.conf = conf;
	}


	public static HTableInterface creatTable(String tableName, String[] familys, int maxVersion) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try{
			if (admin.tableExists(tableName)) {
				System.out.println("table already exists!");
				return (HTableInterface) pool.getTable(tableName);
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor ss = new HColumnDescriptor(familys[i]);

					ss.setMaxVersions(maxVersion);
					tableDesc.addFamily(ss);
				}
				admin.createTable(tableDesc);
				return (HTableInterface) pool.getTable(tableName);
			}
		}finally{
			admin.close();
		}
	}

	public static void deleteTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		} catch (Exception e) {
			logger.error(e);
		}finally{
			admin.close();
		}
	}
	
	public static boolean existsHTable(String tableName) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		try{
			if(admin.tableExists(tableName)){
				return true;
			}else{
				return false;
			}
		}finally{
			admin.close();
		}
	}


	public static HTableInterface getHtable(String tableName) throws IOException
	  {
	    HTableInterface table = null;
	    try {
	      table = pool.getTable(tableName);
	    } catch (Exception e) {
	      logger.error(e);
	    }
	    return table;
	  }


	public static void addRecord(String tableName, byte[] rowKey, String family, String qualifier, String value) throws Exception {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			Put put = new Put(rowKey);
			if (value == null)
				value = "";
			put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
			table.put(put);
		} catch (Exception ex) {
			logger.error(ex);
		} finally {
			returnTable(table);
		}
	}
	
	public static void addRecord(String tableName,Put put){
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			table.put(put);
		} catch (IOException ex) {
			logger.error(ex);
		}finally{
			returnTable(table);
		}
		
	}

	public static void batchRecord(String tableName, List<Put> data) {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			table.put(data);
			// table.flushCommits();
		} catch (Exception ex) {
			logger.error(ex);
		} finally {
			returnTable(table);
		}
	}





	public static void returnTable(HTableInterface table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	public static void delRecord(String tableName, String rowKey) throws Exception {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(rowKey.getBytes());
			list.add(del);
			table.delete(list);
		} catch (Exception ex) {
			logger.error(ex);
		} finally {
			returnTable(table);
		}
	}
	
	/**
	 * 批量删除
	 * @param tableName
	 * @param rowKeyList
	 */
	public static void delRecord(String tableName,List<String> rowKeyList){
		List<Delete> list = new ArrayList<Delete>();
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			for(int i=0;i<rowKeyList.size();i++){
				String row = rowKeyList.get(i);
				Delete del = new Delete(row.getBytes());
						
				list.add(del);
				if(i%100 == 0 && i != 0){
					table.delete(list);
					
					list.clear();
				}
			}
			if(list.size() > 0){
				table.delete(list);				
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			returnTable(table);
		}
	}

	public static ResultScanner findByFilter(String tableName, String family, String qualifier, String value) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);// new HTable(conf,
			// tableName);
			FilterBase course_art_filter = new SingleColumnValueFilter(family.getBytes(), qualifier.getBytes(), CompareOp.EQUAL, value.getBytes());
			Scan s = new Scan();
			s.setCaching(100);
			s.setFilter(course_art_filter);

			return table.getScanner(s);
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
		// scanner = table.getScanner(s);
		/*
		 * for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
		 * for (KeyValue kv : rr.raw()) { System.out.print(new
		 * String(kv.getRow()) + " "); System.out.print(new
		 * String(kv.getFamily()) + ":"); System.out.print(new
		 * String(kv.getQualifier()) + " ");
		 * //System.out.print(kv.getTimestamp() + " "); System.out.println(new
		 * String(kv.getValue())); } }
		 */
		// return scanner;
	}

	public static ResultScanner findBy(String tableName, String startKey, String endKey) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);// new HTable(conf,
			// tableName);
			Scan s = new Scan(startKey.getBytes(), endKey.getBytes());
			
			return table.getScanner(s);
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
	}
	public static ResultScanner findBy(String tableName, String startKey, String endKey,Filter filter) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);// new HTable(conf,
			// tableName);
			Scan s = new Scan(startKey.getBytes(), endKey.getBytes());
			if(filter != null)
				s.setFilter(filter);
			return table.getScanner(s);
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
	}
	public static ResultScanner findBy(String tableName, String key) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);// new HTable(conf,
			// tableName);
			Scan s = new Scan(key.getBytes());
			
			return table.getScanner(s);
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
	}

	public static KeyValue[] getOneRecord(String tableName, byte[] rowKey) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			Get get = new Get(rowKey);
			Result rs = table.get(get);
			return rs.raw();
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
	}

	public static KeyValue[] getOneRecord(String tableName, Get get) throws IOException {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			Result rs = table.get(get);
			return rs.raw();
		} catch (Exception ex) {
			logger.error(ex);
			return null;
		} finally {
			returnTable(table);
		}
	}

	public static ResultScanner getAllRecord(String tableName) {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			Scan s = new Scan();
			s.setCaching(100);
			s.setMaxVersions(5);
			return table.getScanner(s);

		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e);
		}
		return null;
	}

	public static boolean changeMaxVersion(String tableName, int maxVersionNumber) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			HTableDescriptor dd = admin.getTableDescriptor(tableName.getBytes());
			HColumnDescriptor[] hd = dd.getColumnFamilies();
			for (HColumnDescriptor c : hd) {
				c.setMaxVersions(maxVersionNumber);
				admin.modifyColumn(tableName, c);
			}
			admin.enableTable(tableName);
			return true;
		} catch (Exception ex) {
			logger.error(ex);
			return false;
		}finally{
			try {
				if(admin!=null){
					admin.close();
				}
			} catch (IOException e) {
				logger.error(HBaseClusterDao.class,e);
			}
		}
	}

	public static boolean removeBy(String tableName, String startKey, String endKey) {
		HTableInterface table = null;
		try {
			table = getHtable(tableName);
			List<Delete> dList = new ArrayList<Delete>();
			ResultScanner result = findBy(tableName, startKey, endKey);
			if (result != null) {
				for (Result r : result) {
					Delete delete = new Delete(r.getRow());
//					System.out.println(new String (r.getRow()));
					dList.add(delete);
					
					if(dList.size()>500){
						table.delete(dList);
						
						dList.clear();
					}
				}
			}
			table.delete(dList);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e);
		}finally {
			returnTable(table);
		}
		return false;
	}
	
}
