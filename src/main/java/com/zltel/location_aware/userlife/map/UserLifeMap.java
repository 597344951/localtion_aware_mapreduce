package com.zltel.location_aware.userlife.map;

import java.io.IOError;

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
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;

public class UserLifeMap extends TableMapper<Text, Text> {
	private static Logger log = LoggerFactory.getLogger(UserLifeMap.class);

	@Override
	public void map(ImmutableBytesWritable row, Result columns, Context cxt) {
		String value = null;
		try {
			RowData rowdata = HBaseUtil.readResult(columns);
			int size = rowdata.getColumnDatas().size();
			String rowkey = rowdata.getRowkey();
			// 000000075570064_50990000
			String[] rks = rowkey.split("_");
			if (rks.length < 2) {
				log.error(" rowkey 规则不匹配 ,rowkey:" + rowkey + " data:" + Bytes.toStringBinary(row.get()) + "  size:"
						+ rowdata.getColumnDatas().size());
				return;
			}
			String source = rks.length == 2 ? Pointer.SOURCE_CS : Pointer.SOURCE_GN;
			String nettype = null;

			String imsi = new StringBuffer(rks[0]).reverse().toString();

			ColumnData cd = null;
			cd = rowdata.get("info", "stime");
			String stime = cd != null ? cd.getValue() : null;
			cd = rowdata.get("info", "etime");
			String etime = cd != null ? cd.getValue() : null;

			String ci = null;
			String lac = null;
			if (Pointer.SOURCE_GN.equals(source)) {
				nettype = rks[2];
				if ("4".equals(nettype.trim())) {
					cd = rowdata.get("info", "fci");
					ci = cd != null ? cd.getValue() : null;
					cd = rowdata.get("info", "flac");
					lac = cd != null ? cd.getValue() : null;
				} else {
					cd = rowdata.get("info", "ci");
					ci = cd != null ? cd.getValue() : null;
					cd = rowdata.get("info", "lac");
					lac = cd != null ? cd.getValue() : null;
				}

			} else {
				nettype = "2/3";
				cd = rowdata.get("info", "fci");
				ci = cd != null ? cd.getValue() : null;
				cd = rowdata.get("info", "flac");
				lac = cd != null ? cd.getValue() : null;
			}

			Pointer point = new Pointer();
			point.setImsi(imsi);
			point.setSource(source);
			point.setNettype(nettype);
			if (StringUtil.isNotNullAndEmpty(stime) && StringUtil.isNum(stime)) {
				point.setStime(stime);
			}
			if (StringUtil.isNotNullAndEmpty(etime) && StringUtil.isNum(etime)) {
				point.setEtime(etime);
			}
			if (StringUtil.isNotNullAndEmpty(ci)) {
				point.setCi(ci);
			}
			if (StringUtil.isNotNullAndEmpty(lac)) {
				point.setLac(lac);
			}

			if (point.avaliable() && point._stime() != null && point._etime() != null) {
				// log.info(" 处理 imsi:" + imsi);
				String json = JSON.toJSONString(point);
				// log.info(imsi + " -> " + json);
				cxt.write(new Text(imsi), new Text(json));
			} else {
				log.warn(" 数据不完整!  imsi:" + imsi + " 数据:" + rowdata.toString());
			}
		} catch (Exception e) {
			log.error("Map出错:" + e.getMessage() + ",Row:" + Bytes.toStringBinary(row.get()) + ",Value:" + value, e);
			throw new IOError(e);
		}
	}

}
