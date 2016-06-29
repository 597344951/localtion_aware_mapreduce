/**
 * Project Name:BigCount
 * File Name:JobTask.java
 * Package Name:xx.local.mr.upfile
 * Date:2016年3月28日上午10:07:48
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.localUp;


/**
 * ClassName:JobTask <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月28日 上午10:07:48 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class JobTask {
	String[] args = null;

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public JobTask() {
	}

	public JobTask(String[] args) {
		this.args = args;
	}

	public void run() {
		Thread athread = new Thread(new JobThread(args));
		athread.start();
	}

}
