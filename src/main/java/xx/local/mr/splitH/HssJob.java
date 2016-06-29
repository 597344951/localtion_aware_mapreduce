package xx.local.mr.splitH;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class HssJob implements Runnable {
	String[] args = null;

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public HssJob() {

	}

	public HssJob(String[] args) {

		this.args = args;
	}

	public void run() {

		try {
			String jobtype = "3";
			String isFtpWk = null;
			if (args.length > 2) {
				isFtpWk = args[1];
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");

			if ("1".equals(jobtype) || "3".equals(jobtype)) {
				long nlt = 0;
				nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6 * 60
						* 60 * 1000;
				String dated = sdf.format(new Date(nlt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date((nlt)));
				int h = c.get(Calendar.HOUR_OF_DAY);
				System.out.println("new GN ， data ：" + dated + "hour" + h);
				for (int i = 0; i < 2; i++) {
					long bl = nlt - 1 * 60 * 60 * 1000 - i * 60 * 60 * 1000;
					String bldated = sdf.format(new Date(bl));
					Calendar blc = Calendar.getInstance();
					blc.setTime(new Date((bl)));
					int blh = blc.get(Calendar.HOUR_OF_DAY);
					String errmsg = "/home/zltel/hfqs/err/"
							+ bldated.replaceAll("-", "") + "_" + blh + "_"
							+ "gn.txt";
					File gnErrmsg = new File(errmsg);
					if (gnErrmsg.exists()) {
						MainJob.threadLeftOp("+");
						System.out.println(" bl gn  start ， data ：" + bldated
								+ "hour" + blh);
						GnTask blgn = new GnTask(bldated, blh, isFtpWk,
								"errmsg");
						Thread blgnThred = new Thread(blgn);
						blgnThred.start();
						gnErrmsg.delete();
					}
				}
				System.out.println("gn new start");
				MainJob.threadLeftOp("+");
				GnTask gn = new GnTask(dated, h, isFtpWk, "msg");
				Thread gnThred = new Thread(gn);
				gnThred.start();
			}
			if ("2".equals(jobtype) || "3".equals(jobtype)) {
				long nlt = 0;
				nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6 * 60
						* 60 * 1000;
				String dated = sdf.format(new Date(nlt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date((nlt)));
				int h = c.get(Calendar.HOUR_OF_DAY);
				System.out.println("new CS ， data ：" + dated + "hour" + h);
				// csfile = new File("/home/zltel/hfqs/msg/" +
				// dated.replaceAll("-", "")
				// + "_" + h + "_" + "cs.txt");
				for (int i = 0; i < 2; i++) {
					long bl = nlt - 1 * 60 * 60 * 1000 - i * 60 * 60 * 1000;
					String bldated = sdf.format(new Date(bl));
					Calendar blc = Calendar.getInstance();
					blc.setTime(new Date((bl)));
					int blh = blc.get(Calendar.HOUR_OF_DAY);
					String errmsg = "/home/zltel/hfqs/err/"
							+ bldated.replaceAll("-", "") + "_" + blh + "_"
							+ "cs.txt";
					File csErrmsg = new File(errmsg);
					if (csErrmsg.exists()) {
						System.out.println(" bl gn  start ， data ：" + bldated
								+ "hour" + blh);
						MainJob.threadLeftOp("+");
						CsTasek blcs = new CsTasek(bldated, blh, isFtpWk,
								"errmsg");
						Thread blcsThred = new Thread(blcs);
						blcsThred.start();
						csErrmsg.delete();
					}
				}

				MainJob.threadLeftOp("+");
				CsTasek cs = new CsTasek(dated, h, isFtpWk, "msg");
				Thread csThred = new Thread(cs);
				csThred.start();

			}
			long nlt = 0;
			nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 6 * 60 * 60
					* 1000;
			String dated = sdf.format(new Date(nlt));
			Calendar c = Calendar.getInstance();
			c.setTime(new Date((nlt)));
			int h = c.get(Calendar.HOUR_OF_DAY);
			String hfOutGNPath = "/user/zltel/hfOutGN/"
					+ dated.replaceAll("-", "") + "/"
					+ String.format("%02d", h);
			String hfdataOutCS = "/user/zltel/hfdataOutCS/"
					+ dated.replaceAll("-", "") + "/"
					+ String.format("%02d", h);
			for (int i = 0; i < 60; i++) {
				if (MainJob.getThreadLeft() == 0) {
					System.out.println("cs and gn over");
					if ("4".equals(jobtype) || "3".equals(jobtype)) {
						System.out.println("sy start");
						MainJob.threadLeftOp("+");
						HfsyTask sy = new HfsyTask(hfOutGNPath, hfdataOutCS,
								dated, h, isFtpWk);
						Thread syThred = new Thread(sy);
						syThred.start();
					}
					// if ("5".equals(jobtype) || "3".equals(jobtype)) {
					// System.out.println("rl start");
					// MainJob.threadLeftOp("+");
					// HfrlTasek rl = new HfrlTasek(hfOutGNPath, hfdataOutCS,
					// dated, h, isFtpWk);
					// Thread rlThred = new Thread(rl);
					// rlThred.start();
					//
					// }

					break;

				} else {
					Thread.sleep(60 * 1000);
				}

			}
			while (MainJob.getThreadLeft() != 0) {
				Thread.sleep(60 * 1000);
			}
			System.out.println("all over" + dated + "_" + h);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("all over");
		}

	}
}
