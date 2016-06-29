package xx.local.mr.localUp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class JobThread implements Runnable {
	private String[] args = null;

	public JobThread(String[] args) {
		this.args = args;
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public void run() {

		try {

			String jobtype = null;
			String isHdTask = null;
			if (args.length > 2) {
				isHdTask = args[1];
				jobtype = args[2];

			} else {
				return;
			}
			if ("1".equals(jobtype) || "3".equals(jobtype)) {

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
				long nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6
						* 60 * 60 * 1000;
				String dated = sdf.format(new Date(nlt));

				Calendar c = Calendar.getInstance();
				c.setTime(new Date(nlt));
				int h = c.get(Calendar.HOUR_OF_DAY);
				System.out.println("GN_" + dated.replaceAll("-", "") + "_" + h
						+ "开始");
				for (int i = 0; i < 2; i++) {
					long bl = nlt - 1 * 60 * 60 * 1000 - i * 60 * 60 * 1000;
					String bldated = sdf.format(new Date(bl));
					Calendar blc = Calendar.getInstance();
					blc.setTime(new Date((bl)));
					int blh = blc.get(Calendar.HOUR_OF_DAY);
					String gnmsg = "/home/zltel/errmsg/"
							+ bldated.replaceAll("-", "") + "_" + blh
							+ "_gn.txt";
					File gnErrmsg = new File(gnmsg);
					if (gnErrmsg.exists()) {
						MainJob.threadLeftOp("+");
						GnTask blgn = new GnTask(bldated, blh, isHdTask);
						Thread blgnThred = new Thread(blgn);
						blgnThred.start();
						gnErrmsg.delete();
						String newgnmsg = "/home/zltel/msg/"
								+ bldated.replaceAll("-", "") + "_" + blh
								+ "_gn.txt";
						File ngnmsg = new File(newgnmsg);
						ngnmsg.mkdir();
					}
				}
				MainJob.threadLeftOp("+");
				GnTask gn = new GnTask(dated, h, isHdTask);
				Thread gnThred = new Thread(gn);
				gnThred.start();
			}
			if ("2".equals(jobtype) || "3".equals(jobtype)) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
				long nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6
						* 60 * 60 * 1000;
				String dated = sdf.format(new Date(nlt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date(nlt));
				int h = c.get(Calendar.HOUR_OF_DAY);
				System.out.println("CS_" + dated.replaceAll("-", "") + "_" + h
						+ "开始");
				for (int i = 0; i < 2; i++) {
					long bl = nlt - 1 * 60 * 60 * 1000 - i * 60 * 60 * 1000;
					String bldated = sdf.format(new Date(bl));
					Calendar blc = Calendar.getInstance();
					blc.setTime(new Date((bl)));
					int blh = blc.get(Calendar.HOUR_OF_DAY);
					String csmsg = "/home/zltel/errmsg/"
							+ bldated.replaceAll("-", "") + "_" + blh
							+ "_cs.txt";
					File csErrmsg = new File(csmsg);
					if (csErrmsg.exists()) {
						MainJob.threadLeftOp("+");
						CsTask blgn = new CsTask(bldated, blh, isHdTask);
						Thread blgnThred = new Thread(blgn);
						blgnThred.start();
						csErrmsg.delete();
						String newcsmsg = "/home/zltel/msg/"
								+ bldated.replaceAll("-", "") + "_" + blh
								+ "_cs.txt";
						File ncsmsg = new File(newcsmsg);
						ncsmsg.mkdir();
					}
				}
				MainJob.threadLeftOp("+");
				CsTask cs = new CsTask(dated, h, isHdTask);
				Thread csThred = new Thread(cs);
				csThred.start();
			}
			if ("4".equals(jobtype) || "3".equals(jobtype)) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
				long nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6
						* 60 * 60 * 1000;
				String dated = sdf.format(new Date(nlt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date(nlt));
				int h = c.get(Calendar.HOUR_OF_DAY);
				System.out.println("SY_" + dated.replaceAll("-", "") + "_" + h
						+ "开始");
				MainJob.threadLeftOp("+");
				SyTask sy = new SyTask(dated, h, isHdTask);
				Thread syThred = new Thread(sy);
				syThred.start();
			}
			// if ("5".equals(jobtype) || "3".equals(jobtype)) {
			// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			// SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
			// long nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 4
			// * 60 * 60 * 1000;
			// String dated = sdf.format(new Date(nlt));
			// Calendar c = Calendar.getInstance();
			// c.setTime(new Date(nlt));
			// int h = c.get(Calendar.HOUR_OF_DAY);
			// System.out.println("RL_" + dated.replaceAll("-", "") + "_" + h
			// + "开始");
			// MainJob.threadLeftOp("+");
			// RlTasek rl = new RlTasek(dated, h, isHdTask);
			// Thread rlThred = new Thread(rl);
			// rlThred.start();
			// }

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
			long nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6 * 60
					* 60 * 1000;
			String alldata = sdf.format(new Date(nlt));
			Calendar c = Calendar.getInstance();
			c.setTime(new Date(nlt));
			int allh = c.get(Calendar.HOUR_OF_DAY);
			for (int i = 0; i < 600; i++) {
				if (MainJob.getThreadLeft() == 0) {
					break;
				} else {
					Thread.sleep(60 * 1000);
				}
				if (i % 60 == 0) {
					System.out.println("ALL_" + alldata.replaceAll("-", "")
							+ "_" + allh + "一个小时");
				}
			}
			System.out.println("job all over" + alldata.replaceAll("-", "")
					+ "_" + allh);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("job all over");
		}

	}

}
