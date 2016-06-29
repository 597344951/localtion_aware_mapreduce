/**
 * Project Name:BigCount
 * File Name:MainJob.java
 * Package Name:xx.local.mr.localUp
 * Date:2016年4月7日下午5:01:20
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;

/**
 * ClassName:MainJob <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年4月7日 下午5:01:20 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class MainJob {
	private static Integer threadLeft = 0;// 线程剩余数量

	public static Integer getThreadLeft() {
		return threadLeft;
	}

	public static void setThreadLeft(Integer threadLeft) {
		MainJob.threadLeft = threadLeft;
	}

	public static synchronized void threadLeftOp(String op) {
		if ("+".equals(op)) {
			threadLeft++;
		} else if ("-".equals(op)) {
			threadLeft--;
		}
	}

	public static void main(String[] args) {

		String type = "";
		if (args.length > 2) {
			type = args[0];
		}
		if ("1".equals(type) || "3".equals(type)) {
			DataCollection dataColl = new DataCollection(args);
			Thread dataCollThred = new Thread(dataColl);
			dataCollThred.start();
		}
		if ("2".equals(type) || "3".equals(type)) {
			JobTask job = new JobTask(args);
			job.run();
			// Timer timer = new Timer();
			// timer.schedule(new JobTask(args), 1000, 60 * 60000);
		}

	}
}
