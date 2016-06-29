package com.zltel.location.test.main;

public class Ttt implements Runnable {
	public void run() {
		System.err.println("凯斯");
		try {
			Thread.sleep(2*60 * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.println("over");
	}
}
