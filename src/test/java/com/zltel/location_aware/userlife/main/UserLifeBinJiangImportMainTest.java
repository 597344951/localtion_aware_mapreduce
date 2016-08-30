package com.zltel.location_aware.userlife.main;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;

public class UserLifeBinJiangImportMainTest {
	List<String> args = new ArrayList<String>();

	@Before
	public void setUp() throws Exception {
		// userlilfe_import/testData/ userlife_05 1 10 5
		UserLifeBJ_DoubleSortMain.BASE_PATH = "D:\\HDFS_HOME\\user\\zltel\\";
		UserLifeBinJiangImportMain.RUN_DEBUG = true;
		//
		args.add(UserLifeBJ_DoubleSortMain.BASE_PATH + "userlife\\testImport\\");
		args.add("userlife_05");
		args.add("1");
		args.add("0");
		args.add("5");
	}

	@After
	public void tearDown() throws Exception {
	}

	public final void test() {
		String[] _args = new String[args.size()];
		args.toArray(_args);
		try {
			UserLifeBinJiangImportMain.main(_args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
