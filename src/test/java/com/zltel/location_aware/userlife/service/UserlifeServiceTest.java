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
import com.zltel.dbscan.bean.Point;
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

	public final void testanalyseHome() throws Exception {
		testfilter();
		List<TopPointer> topPointers = UserlifeService.analyseHome(pointers, 0);
		System.out.println("居住地列表");
		for (int i = 0; i < topPointers.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + topPointers.get(i));
		}
	}

	@Test
	public final void testanalyseWork() throws Exception {
		long start = System.currentTimeMillis();
		testfilter();
		List<TopPointer> topPointers = UserlifeService.analyseWork(pointers, 0);
		System.out.println("工作地列表");
		for (int i = 0; i < topPointers.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + topPointers.get(i));
		}
		List<TopPointer> home = UserlifeService.analyseHome(pointers, 0);
		System.out.println("居住地列表");
		for (int i = 0; i < home.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + home.get(i));
		}
		List<TopPointer> fun = UserlifeService.analyseFun(pointers, 0);
		System.out.println("娱乐地地列表");
		for (int i = 0; i < fun.size(); i++) {
			System.out.println(" 第 " + i + "个点: " + fun.get(i));
		}
		start = System.currentTimeMillis() - start;
		System.out.println("总耗时:" + start);
	}

	public final void testCreatePut() throws Exception {
		testfilter();
		List<TopPointer> home = UserlifeService.analyseHome(pointers, 0);
		List<TopPointer> work = UserlifeService.analyseWork(pointers, 0);

		List<Put> homeputs = UserlifeService.createPuts("460000802503270", home, UserlifeService.TYPE_HOME, null, null);
		List<Put> workputs = UserlifeService.createPuts("460000802503270", work, UserlifeService.TYPE_WORK, null, null);

		System.out.println(homeputs);
		System.out.println(workputs);
	}

	@Test
	public void testCountPointersInfo() throws Exception {
		TopPointer tp = new TopPointer();
		List<Point> _ps = new ArrayList<Point>();
		List<Pointer> ps = new ArrayList<Pointer>();
		String json = "{\"appid\":\"17\",\"business_type\":\"data\",\"cell\":\"长命SM_1\",\"ci\":\"184624129\",\"down_bytes\":\"187573\",\"etime\":\"20160501133054\",\"groupCount\":34,\"host\":\"api.ra2.xlmc.sec.miui.com\",\"imei\":\"867463021427218\",\"imsi\":\"460079696249810\",\"lac\":\"22565\",\"lat\":30.38594,\"lng\":120.00106,\"nettype\":\"4\",\"score\":15280,\"source\":\"gn\",\"stime\":\"20160501133019\",\"tag\":\"AW\",\"tagDescript\":\"下午工作时段\",\"time\":0,\"timeRanges\":[\"20160501133125-20160501133854\",\"20160501133153-20160501133625\",\"20160501133155-20160501133247\",\"20160501133205-20160501133247\",\"20160501133625-20160501134133\",\"20160501135239-20160501140140\",\"20160501135844-20160501140140\",\"20160501140140-20160501142638\",\"20160501140611-20160501141307\",\"20160501141307-20160501141738\",\"20160501141738-20160501142208\",\"20160501142208-20160501142638\",\"20160501142209-20160501143023\",\"20160501142638-20160501144505\",\"20160501143109-20160501144032\",\"20160501144032-20160501144505\",\"20160501145234-20160501150612\",\"20160501145705-20160501150140\",\"20160501150140-20160501150612\",\"20160501151739-20160501153544\",\"20160501152210-20160501152641\",\"20160501152641-20160501153113\",\"20160501153113-20160501153544\",\"20160501153544-20160501163147\",\"20160501154309-20160501154739\",\"20160501154739-20160501155210\",\"20160501155210-20160501155640\",\"20160501155640-20160501160142\",\"20160501160142-20160501160614\",\"20160501160614-20160501161308\",\"20160501161308-20160501161739\",\"20160501161739-20160501162642\",\"20160501162642-20160501163147\",\"20160501162929-20160501163626\"],\"up_bytes\":\"6065\",\"url\":\"api.ra2.xlmc.sec.miui.com/api/expression/shake?callId=1462080620090&v=1.0&sig=797f987345aecb507f09d5b9b8075165&appId=3\",\"weekday\":false,\"weight\":1}";
		Pointer p = JSON.toJavaObject(JSON.parseObject(json), Pointer.class);
		_ps.add(new Point(p));
		ps.add(p);
		tp.setPointers(ps);
		UserlifeService.CountPointersInfo(tp, _ps);
		System.out.println(tp);
	}

}
