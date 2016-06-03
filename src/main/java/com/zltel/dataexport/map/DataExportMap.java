package com.zltel.dataexport.map;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.common.utils.hbase.HBaseUtil.ColumnData;
import com.zltel.common.utils.hbase.HBaseUtil.RowData;

public class DataExportMap extends TableMapper<Text, Text> {
	private static Logger log = LoggerFactory.getLogger(DataExportMap.class);

	@Override
	public void map(ImmutableBytesWritable row, Result columns, Context cxt) {
		String value = null;
		try {
			RowData rowdata = HBaseUtil.readResult(columns);
			String rowkey = rowdata.getRowkey();
			List<ColumnData> cs = rowdata.getColumnDatas();
			cxt.write(new Text(rowkey), new Text(JSON.toJSONString(cs)));
		} catch (Exception e) {
			log.error("出错:" + e.getMessage() + ",Row:" + Bytes.toStringBinary(row.get()) + ",Value:" + value, e);
		}
	}

}
