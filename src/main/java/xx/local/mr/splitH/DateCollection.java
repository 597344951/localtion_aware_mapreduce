/**
 * Project Name:BigCount
 * File Name:GnCollection.java
 * Package Name:xx.local.mr.splitH
 * Date:2016年3月27日下午3:59:42
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ClassName:GnCollection <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月27日 下午3:59:42 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class DateCollection implements Runnable {
	String args[] = null;
	private static Log log = LogFactory.getLog(DateCollection.class);

	public DateCollection() {
	}

	public DateCollection(String[] args) {
		this.args = args;
	}

	public void run() {

		try {
			String startDate = null;
			String endDate = null;
			String jobtype = null;
			String isFtpWk = null;
			if (args.length > 4) {
				isFtpWk = args[1];
				jobtype = args[2];
				startDate = args[3];
				endDate = args[4];
			} else {
				return;
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");

			long nlt = sdfh.parse(sdfh.format(new Date())).getTime();
			long olt = sdfh.parse(startDate).getTime();
			long elt = sdfh.parse(endDate).getTime();
			while (nlt > olt && olt <= elt) {
				String dated = sdf.format(new Date(olt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date(olt));
				int h = c.get(Calendar.HOUR_OF_DAY);
				log.info("data collection start ， data ：" + dated + "hour" + h);
				olt += 60 * 60 * 1000;
				if ("1".equals(jobtype) || "3".equals(jobtype)) {
					log.info("gn coll start");
					MainJob.threadLeftOp("+");
					GnTask gn = new GnTask(dated, h, isFtpWk, "msg");
					Thread gnThred = new Thread(gn);
					gnThred.start();
				}
				if ("2".equals(jobtype) || "3".equals(jobtype)) {
					log.info("cs coll start");
					MainJob.threadLeftOp("+");
					CsTasek cs = new CsTasek(dated, h, isFtpWk, "msg");
					Thread gnThred = new Thread(cs);
					gnThred.start();
				}

				String hfOutGNPath = "/user/zltel/hfOutGN/"
						+ dated.replaceAll("-", "") + "/"
						+ String.format("%02d", h);
				String hfdataOutCS = "/user/zltel/hfdataOutCS/"
						+ dated.replaceAll("-", "") + "/"
						+ String.format("%02d", h);
				for (int i = 0; i < 60; i++) {
					if (MainJob.getThreadLeft() == 0) {
						log.info("时间：" + dated + "小时：" + h + "gn or cs 线程结束");
						if ("4".equals(jobtype) || "3".equals(jobtype)) {
							log.info("sy start");
							MainJob.threadLeftOp("+");
							HfsyTask sy = new HfsyTask(hfOutGNPath,
									hfdataOutCS, dated, h, isFtpWk);
							Thread syThred = new Thread(sy);
							syThred.start();
						}
						// if ("5".equals(jobtype) || "3".equals(jobtype)) {
						// log.info("rl start");
						// threadLeftOp("+");
						// HfrlTasek rl = new HfrlTasek(hfOutGNPath,
						// hfdataOutCS, dated, h,
						// isFtpWk);
						// Thread rlThred = new Thread(rl);
						// rlThred.start();
						// }
						break;
					} else {
						Thread.sleep(60 * 1000);
					}

				}
				for (int i = 0; i < 60; i++) {
					if (MainJob.getThreadLeft() == 0) {
						log.info("时间：" + dated + "小时：" + h + "全部结束");
						break;
					} else {
						Thread.sleep(60 * 1000);
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
