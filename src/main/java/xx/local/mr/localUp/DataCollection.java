/**
 * Project Name:BigCount
 * File Name:GnCollection.java
 * Package Name:xx.local.mr.splitH
 * Date:2016年3月27日下午3:59:42
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.mortbay.log.Log;

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
public class DataCollection implements Runnable {
	String args[] = null;

	public DataCollection() {
	}

	public DataCollection(String[] args) {
		this.args = args;
	}

	public void run() {

		try {
			String startDate = null;
			String endDate = null;
			String jobtype = null;
			String isHdTask = null;
			if (args.length > 4) {
				isHdTask = args[1];
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
				olt += 60 * 60 * 1000;
				// String gnsucmsg = "/home/zltel/sucmsg/" +
				// dated.replaceAll("-", "")
				// + "_" + h + "_gn.txt";
				// File gns = new File(gnsucmsg);
				// String cssucmsg = "/home/zltel/sucmsg/" +
				// dated.replaceAll("-", "")
				// + "_" + h + "_cs.txt";
				// File css = new File(cssucmsg);
				// String sysucmsg = "/home/zltel/sucmsg/" +
				// dated.replaceAll("-", "")
				// + "_" + h + "_sy.txt";
				// File sys = new File(sysucmsg);
				if ("1".equals(jobtype) || "3".equals(jobtype)) {
					MainJob.threadLeftOp("+");
					GnTask gn = new GnTask(dated, h, isHdTask);
					Thread gnThred = new Thread(gn);
					gnThred.start();
				}
				if ("2".equals(jobtype) || "3".equals(jobtype)) {
					MainJob.threadLeftOp("+");
					CsTask cs = new CsTask(dated, h, isHdTask);
					Thread csThred = new Thread(cs);
					csThred.start();
				}
				if ("4".equals(jobtype) || "3".equals(jobtype)) {
					MainJob.threadLeftOp("+");
					SyTask sy = new SyTask(dated, h, isHdTask);
					Thread syThred = new Thread(sy);
					syThred.start();
				}

				for (int i = 0; i < 600; i++) {
					if (MainJob.getThreadLeft() == 0) {
						Log.info("job all over");
						break;
					} else {
						Thread.sleep(60 * 1000);
					}
					if (i % 60 == 0) {
						System.out.println("ALL_" + dated.replaceAll("-", "")
								+ "_" + h + "一个小时");
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
