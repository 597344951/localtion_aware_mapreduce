package com.zltel.location_aware.userlife.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.main.UserLifeBinJiangTestDataMain;

public class UserLifeBinJiangTestDataMap extends UserLifeBinJiangMap {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangTestDataMap.class);

	private static java.util.Set<String> filter_imsis = null;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		if (filter_imsis == null) {
			filter_imsis = new HashSet<String>();
			// 读取 gis 数据
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = null;
			try {
				// 输出全部文件内容
				in = fs.open(new Path(UserLifeBinJiangTestDataMain.FILTER_IMSI));
				// 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				IOUtils.copyBytes(in, bo, 50, false);
				String txt = new String(bo.toByteArray(), "UTF-8");
				if (StringUtil.isNotNullAndEmpty(txt)) {
					String[] lines = txt.split("\r\n");
					for (String imsi : lines) {
						if (StringUtil.isNotNullAndEmpty(imsi)) {
							filter_imsis.add(imsi.trim());
						}
					}
				}
			} finally {
				IOUtils.closeStream(in);
			}
		}

	}

	protected void map(LongWritable _key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Pointer point = createPointer(value);

		if (point != null && point.avaliable() && point._stime() != null && point._etime() != null) {
			String imsi = point.getImsi();
			if (checkIMSI(imsi)) {
				context.write(new Text(point.getImsi()), value);
			}
		}
	}

	/**
	 * 检测 IMSI 是否符合规则
	 * 
	 * @param imsi
	 * @return true: 符合，false:不符合
	 */
	private boolean checkIMSI(String imsi) {
		if (StringUtil.isNullOrEmpty(imsi)) {
			return false;
		}
		return filter_imsis.contains(imsi.trim());
	}

	public static void main(String[] args) {

	}
}
