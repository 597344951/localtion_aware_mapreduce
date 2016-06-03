package com.zltel.location_aware.userlife.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.file.FileReadHandle;
import com.zltel.common.utils.file.FileReadUtil;
import com.zltel.dbscan.service.Dbscan;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.bean.TopPointer;

public class UserlifeServiceTest {
	List<Pointer> pointers = new ArrayList<Pointer>();

	@Before
	public void setUp() throws Exception {
		long start = System.currentTimeMillis();
		String fn = ConfigUtil.CLASSPATH + "/test/userlife.txt";
		FileReadUtil.readFileAndHandle(FileReadUtil.getFileReader(new File(fn), FileReadUtil.CHARSET_UTF8),
				new FileReadHandle() {
					private String before = null;
					private long _time = 0;

					public void handleLine(String line) {
						try {
							// System.out.println(line);
							String imsi = line.split("\t")[0];

							String js = line.split("\t")[1];
							Pointer p = JSON.toJavaObject(JSON.parseObject(js), Pointer.class);
							if (_time == 0) {
								_time = UserlifeService.daysdf.parse(UserlifeService.daysdf.format(p._stime()))
										.getTime();
							}
							long _stl = 0;
							long _etl = 0;
							if (imsi.equals(before)) {
								_stl = _time + p._stime().getTime() - UserlifeService.daysdf
										.parse(UserlifeService.daysdf.format(p._stime())).getTime();
								_etl = _time + p._etime().getTime() - UserlifeService.daysdf
										.parse(UserlifeService.daysdf.format(p._etime())).getTime();
							} else {
								_time += 24 * 3600 * 1000;
								before = imsi;

								_stl = _time + p._stime().getTime() - UserlifeService.daysdf
										.parse(UserlifeService.daysdf.format(p._stime())).getTime();
								_etl = _time + p._etime().getTime() - UserlifeService.daysdf
										.parse(UserlifeService.daysdf.format(p._etime())).getTime();
							}
							String st = UserlifeService.sdf.format(new Date(_stl));
							String et = UserlifeService.sdf.format(new Date(_etl));

							p.setStime(st);
							p.setEtime(et);

							pointers.add(p);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
		start = System.currentTimeMillis() - start;
		System.out.println("加载文件耗时: " + start);
	}

	@After
	public void tearDown() throws Exception {
	}

	public final void testMergePointers() {
		UserlifeService.sortPointers(pointers);
		List<Pointer> nl = UserlifeService.mergePointers(pointers);
		System.out.println("合并前： " + pointers.size() + "\t 合并后:" + nl.size());
		pointers = nl;
	}

	public final void testfilter() {
		testMergePointers();
		List<Pointer> nl = UserlifeService.filter(pointers);
		System.out.println("过滤合并前： " + pointers.size() + "\t 过滤合并后:" + nl.size());
		Dbscan db = Dbscan.getInstince();
		db.getResult(nl);
		// db.display();
		pointers = nl;
	}

	public final void testanalyseHome() {
		testfilter();
		List<TopPointer> topPointers = UserlifeService.analyseHome(pointers);
		System.out.println("居住地列表");
		for (int i = 0; i < topPointers.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + topPointers.get(i));
		}
	}

	@Test
	public final void testanalyseWork() {
		long start = System.currentTimeMillis();
		testfilter();
		List<TopPointer> topPointers = UserlifeService.analyseWork(pointers);
		System.out.println("工作地列表");
		for (int i = 0; i < topPointers.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + topPointers.get(i));
		}
		List<TopPointer> home = UserlifeService.analyseHome(pointers);
		System.out.println("居住地列表");
		for (int i = 0; i < home.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + home.get(i));
		}
		List<TopPointer> fun = UserlifeService.analyseFun(pointers);
		System.out.println("娱乐地地列表");
		for (int i = 0; i < fun.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + fun.get(i));
		}
		start = System.currentTimeMillis() - start;
		System.out.println("总耗时:" + start);
	}

	public final void testCreatePut() {
		testfilter();
		List<TopPointer> home = UserlifeService.analyseHome(pointers);
		List<TopPointer> work = UserlifeService.analyseWork(pointers);

		List<Put> homeputs = UserlifeService.createPuts("460000802503270", home, UserlifeService.TYPE_HOME, null, null);
		List<Put> workputs = UserlifeService.createPuts("460000802503270", work, UserlifeService.TYPE_WORK, null, null);

		System.out.println(homeputs);
		System.out.println(workputs);
	}

}
