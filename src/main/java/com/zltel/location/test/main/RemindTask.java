package com.zltel.location.test.main;

import java.util.TimerTask;

public class RemindTask extends TimerTask {
	public void run() {
		Thread t = new Thread(new Ttt());
		t.start();
	}

}
