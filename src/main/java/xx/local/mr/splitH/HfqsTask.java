package xx.local.mr.splitH;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HfqsTask extends TimerTask {
	String[] args = null;

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public HfqsTask() {
	}

	public HfqsTask(String[] args) {
		this.args = args;
	}

	@Override
	public void run() {
		try {

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
			long nlt = 0;
			nlt = sdfh.parse(sdfh.format(new Date())).getTime() - 4 * 60 * 60
					* 1000;
			String dated = sdf.format(new Date(nlt));
			Calendar c = Calendar.getInstance();
			c.setTime(new Date((nlt)));
			int h = c.get(Calendar.HOUR_OF_DAY);
			System.out.println("new start ， data ：" + dated + "hour" + h);
			Thread athread = new Thread(new HssJob(args));
			athread.start();

		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
