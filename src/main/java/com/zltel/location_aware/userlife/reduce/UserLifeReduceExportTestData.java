package com.zltel.location_aware.userlife.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.utils.PointsLocationCheckUtil;

public class UserLifeReduceExportTestData extends Reducer<Text, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeReduceExportTestData.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> iterator, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text value : iterator) {
			try {
				String json = value.toString();
				Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
				p = PointsLocationCheckUtil.check(p);
				if (p != null) {
					String j = JSON.toJSONString(p);
					context.write(key, new Text(j));
				} else {
					logout.warn("没有识别到数据! " + json);
				}
			} catch (Exception e) {
				logout.error(e.getMessage(), e);
			}
		}
	}

}
