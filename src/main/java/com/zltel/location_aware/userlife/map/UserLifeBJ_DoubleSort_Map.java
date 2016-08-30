package com.zltel.location_aware.userlife.map;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.doublesort.DoubleSortKey;
import com.zltel.location_aware.userlife.service.UserlifeService;

/**
 * 用户标签 使用双重 缓冲 Map
 * 
 * @author Wangch
 *
 */
public class UserLifeBJ_DoubleSort_Map extends Mapper<LongWritable, Text, DoubleSortKey, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBJ_DoubleSort_Map.class);

	UserLifeBinJiangMap bm = null;

	@Override
	protected void setup(Mapper<LongWritable, Text, DoubleSortKey, Text>.Context context)
			throws IOException, InterruptedException {
		bm = new UserLifeBinJiangMap();
	}

	protected void map(LongWritable _key, Text value, Mapper<LongWritable, Text, DoubleSortKey, Text>.Context context)
			throws IOException, InterruptedException {
		Pointer point = bm.createPointer(value);
		if (point != null && point.avaliable() && point._stime() != null && point._etime() != null) {
			UserlifeService.markTags(point);// 增加 tag
			if (StringUtil.isNullOrEmpty(point.getTag())) {
				return;
			}
			String imsi = point.getImsi();
			Date _st = point._stime();
			if (null == imsi || null == _st) {
				return;
			}
			String json = JSON.toJSONString(point);
			Text _value = new Text(json);

			int type = 0;
			if (UserlifeService.isHomeTag(point)) {
				type = UserlifeService.TYPE_HOME;
				DoubleSortKey dsk = mkSortKey(point, type);
				context.write(dsk, _value);
			}
			if (UserlifeService.isWorkTag(point)) {
				type = UserlifeService.TYPE_WORK;
				DoubleSortKey dsk = mkSortKey(point, type);
				context.write(dsk, _value);
			}
			if (UserlifeService.isFunTag(point)) {
				type = UserlifeService.TYPE_FUN;
				DoubleSortKey dsk = mkSortKey(point, type);
				context.write(dsk, _value);
			}
		}
	}

	private DoubleSortKey mkSortKey(Pointer pointer, int type) {
		String imsi = pointer.getImsi() + "_" + type;
		DoubleSortKey dsk = new DoubleSortKey();
		dsk.setFirstKey(new Text(imsi));
		dsk.setSecondKey(new LongWritable(pointer._stime().getTime()));
		return dsk;
	}
}
