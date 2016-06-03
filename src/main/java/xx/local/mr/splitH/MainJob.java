/**
 * Project Name:BigCount
 * File Name:HfGnJob.java
 * Package Name:xx.local.mr.splitH
 * Date:2016年3月27日下午3:38:53
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.util.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ClassName:HfGnJob <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016年3月27日 下午3:38:53 <br/>
 * 
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 * 
 * 
 * 
 * 
 * 
 */

public class MainJob {
  private static Log log = LogFactory.getLog(MainJob.class);
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
      log.info("dataCollection start");
      DateCollection dateColl = new DateCollection(args);
      Thread dataThred = new Thread(dateColl);
      dataThred.start();
    }
    if ("2".equals(type) || "3".equals(type)) {
      log.info("timer start start");
      Timer timer = new Timer();
      timer.scheduleAtFixedRate(new HfqsTask(args), 1000, 60 * 60000);
    }
  }
}
