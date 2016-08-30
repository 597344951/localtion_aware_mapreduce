package com.zltel.location_aware.userlife.map;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.main.UserLifeBinJiangMain;

public class UserLifeBinJiangMap extends Mapper<LongWritable, Text, Text, Text> {
	private static Logger logout = LoggerFactory.getLogger(UserLifeBinJiangMap.class);

	private int startRegion = 0;
	private int endRegion = 99;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration config = context.getConfiguration();
		String ssr = config.get(UserLifeBinJiangMain.STR_STARTREGION);
		String esr = config.get(UserLifeBinJiangMain.STR_ENDREGION);
		if (StringUtil.isNotNullAndEmpty(ssr) && StringUtil.isNum(ssr)) {
			startRegion = Integer.valueOf(ssr);
		}
		if (StringUtil.isNotNullAndEmpty(esr) && StringUtil.isNum(esr)) {
			endRegion = Integer.valueOf(esr);
		}

	}

	/**
	 * 创建 Pointer
	 * 
	 * @param _key
	 * @param value
	 * @param context
	 * @return
	 */
	public Pointer createPointer(Text value) {
		String[] strs = value.toString().trim().split("\\t");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		// SimpleDateFormat sdfd = new SimpleDateFormat("yyyyMMdd");
		String[] colum = null;
		if (strs.length > 1) {
			String ci = null;
			String rat = null;
			String type = null; // 网络类型
			String lac = null;
			String source = null;
			String stime = null;
			String etime = null;
			String imsi = null;
			String imei = null;
			String bussiness_type = null;
			String up_bytes = null;
			/** 下载 字节数 **/
			String down_bytes = null;
			String appid = null;
			String host = null;
			String url = null;

			String[] ss = strs[0].trim().split("_");
			if (ss.length != 2) {
				return null;
			}
			colum = (strs[1].trim() + "1").split("\\|");
			if (colum.length >= 39) {
				// GN
				source = Pointer.SOURCE_GN;
				ci = Integer.parseInt("".equals(colum[11]) ? "0" : colum[11], 16) + "";

				rat = colum[7];
				if ("6".equals(rat)) {
					type = "4";
				} else if ("1".equals(rat)) {
					type = "2";
				} else if ("2".equals(rat)) {
					type = "3";
				}
				stime = sdf.format(new Date(Long.parseLong(colum[0]) * 1000));
				etime = sdf.format(new Date(Long.parseLong(colum[1]) * 1000));
				imei = colum[3];
				imsi = colum[4];
				rat = colum[7];
				lac = Integer.parseInt("".equals(colum[10]) ? "0" : colum[10], 16) + "";
				if (colum.length >= 22)
					up_bytes = colum[21];
				if (colum.length >= 23)
					down_bytes = colum[22];

				if (colum.length >= 19)
					appid = colum[18];
				if (colum.length >= 25)
					host = colum[24];
				if (colum.length >= 26)
					url = colum[25];

				// 16 进制 -> 10进制
				if ("1".equals(rat)) {// 3G
					ci = Integer.parseInt("".equals(colum[12]) ? "0" : colum[12], 16) + "";
				} else if ("2".equals(rat)) {// 2G
					ci = Integer.parseInt("".equals(colum[11]) ? "0" : colum[11], 16) + "";
				}

				// 4g网络
				if ("4".equals(type)) {
					lac = Integer.parseInt("".equals(colum[colum.length - 3]) ? "0" : colum[colum.length - 3], 16) + "";
					ci = Integer.parseInt("".equals(colum[colum.length - 2]) ? "0" : colum[colum.length - 2], 16) + "";
				}
				bussiness_type = Pointer.BUSSINESS_DATA;

			} else {
				// CS
				source = Pointer.SOURCE_CS;
				colum = strs[1].trim().split(",");
				if (ss[0].length() > 0 && colum.length > 15) {
					type = "2";// 2g
					imsi = colum[1];
					stime = colum[6];
					etime = colum[7];
					lac = colum[8];
					ci = colum[9];

					String cdr_id = colum[12];
					String call_type = colum[13];
					StringBuffer cimsi = new StringBuffer();
					StringBuffer dimsi = new StringBuffer();

					String cimei = colum[4];
					String cdimei = colum[5];

					cimsi.append(colum[0]);
					dimsi.append(colum[1]);

					// String cnum = colum[2];
					// String dnum = colum[3];

					// 通话 2G：0 3G： 1
					if ("0".equals(cdr_id) || "1".equals(cdr_id)) {
						// "呼叫业务类型
						// 0:主叫流程MOC；
						// 1:被叫流程MTC；
						// 2:切入呼叫；
						// 3:紧急呼叫；
						// 4:业务重建；
						// 5:MO切入呼叫——Utrace关联切入呼叫和MO记录后填值；
						// 6:MT切入呼叫——Utrace关联切入呼叫和MT记录后填值。"

						// 主叫
						if ("0".equals(call_type) || "2".equals(call_type) || "3".equals(call_type)
								|| "4".equals(call_type) || "5".equals(call_type)) {
							imsi = cimsi.toString();
							imei = cimei;
						} else if ("1".equals(call_type) || "6".equals(call_type)) {
							// 被叫
							imsi = dimsi.toString();
							imei = cdimei;
						}
						bussiness_type = Pointer.BUSSINESS_VOICE;
						type = "0".equals(cdr_id) ? "2" : "1".equals(cdr_id) ? "3" : "";

						// 位置更新， 2G：2 3G： 3
					} else if ("2".equals(cdr_id) || "3".equals(cdr_id)) {
						imsi = cimsi.toString();
						imei = cimei;
						bussiness_type = Pointer.BUSSINESS_LOCATION;
						type = cdr_id;
						// 短信 2G：4 3G： 5
					} else if ("4".equals(cdr_id) || "5".equals(cdr_id)) {
						type = "4".equals(cdr_id) ? "2" : "5".equals(cdr_id) ? "3" : "";
						if ("0".equals(call_type) || "3".equals(call_type)) {
							imsi = cimsi.toString();
							imei = cimei;
						} else if ("1".equals(call_type) || "2".equals(call_type) || "4".equals(call_type)) {
							imsi = dimsi.toString();
							imei = cimei;
						}
						bussiness_type = Pointer.BUSSINESS_SMS;
					}
				}
			}

			// 判断是否符合
			if (!checkIMSI(imsi)) {
				return null;
			}

			// 创建 pointer
			Pointer point = new Pointer();
			point.setImsi(imsi);
			point.setSource(source);
			point.setNettype(type);
			point.setBusiness_type(bussiness_type);
			point.setImsi(imsi);
			point.setImei(imei);
			point.setUp_bytes(up_bytes);
			point.setDown_bytes(down_bytes);
			point.setAppid(appid);
			point.setHost(host);
			point.setUrl(url);

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
			return point;
		}
		return null;
	}

	protected void map(LongWritable _key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Pointer point = createPointer(value);

		if (point != null && point.avaliable() && point._stime() != null && point._etime() != null) {
			String json = JSON.toJSONString(point);
			context.write(new Text(point.getImsi()), new Text(json));
			// logout.info("imsi:" + imsi + "\njson:" + json);
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
		String cl = imsi.substring(imsi.length() - 2);
		if (StringUtil.isNum(cl)) {
			int _cl = Integer.valueOf(cl);
			if (startRegion == endRegion) {
				return startRegion == _cl;
			} else {
				return startRegion < _cl && _cl <= endRegion;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		UserLifeBinJiangMap ulbjm = new UserLifeBinJiangMap();
		boolean f = ulbjm.checkIMSI("13434515413434");
		System.out.println(f);
	}
}
