package com.zltel.location_aware.test.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.hbase.HBaseConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.common.utils.hbase.HBaseUtil.ColumnData;

public class CreateData {
	static Configuration config = null;

	public static void main(String[] args) {
		Map<String, String> _confMap = ConfigUtil.resolveConfigProFile("hadoop.properties");
		String hdfs = ConfigUtil.getConfigValue(_confMap, "fs.defaultFS", "hdfs://dn1:8020");
		String hbase = ConfigUtil.getConfigValue(_confMap, "hbase.master", "dn1:60000");
		String zookeeper = ConfigUtil.getConfigValue(_confMap, "hbase.zookeeper.quorum", "dn1");
		config = HBaseConfigUtil.createConfig(hdfs, hbase, zookeeper);

		createData2();
	}

	private static void createData() {
		Connection con = null;
		try {
			String imsi = new StringBuffer("460007163622789").reverse().toString();
			con = HBaseUtil.getConnection(config);
			Table table = HBaseUtil.getTable(con, "hf_csql_20160421");

			for (int _c = 0; _c < 100; _c++) {
				String rowkey = imsi.concat("_" + System.currentTimeMillis());
				List<ColumnData> datas = new ArrayList<HBaseUtil.ColumnData>();
				for (int i = 0; i < 10; i++) {
					ColumnData cd = new ColumnData("info", "q" + i, "" + System.currentTimeMillis() % 100);
					datas.add(cd);
				}

				Put put = HBaseUtil.createPut(rowkey, datas);
				table.put(put);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			HBaseUtil.close(con);
		}
	}

	private static void createData2() {
		Connection con = null;
		try {
			HBaseClient hbaseClient = new HBaseClient("dn1");
			String imsi = new StringBuffer("460007163622789").reverse().toString();

			PutRequest pr = new PutRequest(Bytes.toBytes("hf_csql_20160421"), imsi.getBytes(), "info".getBytes(),
					"q1".getBytes(), "100".getBytes());
			hbaseClient.put(pr);
			hbaseClient.shutdown().joinUninterruptibly();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			HBaseUtil.close(con);
		}
	}
}
