package com.zltel.location_aware.userlife.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.conf.ConfigUtil;
import com.zltel.common.utils.date.DateUtil;
import com.zltel.common.utils.hbase.HBaseConfigUtil;
import com.zltel.common.utils.hbase.HBaseUtil;
import com.zltel.common.utils.string.StringUtil;
import com.zltel.location_aware.userlife.map.UserLifeMap;
import com.zltel.location_aware.userlife.reduce.UserLifeReduce;

/**
 * 用户 标签识别 主入口
 * 
 * @author Wangch
 *
 */
public class UserLifeMain {
	private static Logger logout = LoggerFactory.getLogger(UserLifeMain.class);
	public static final String cs_ql = "hf_csql_";
	public static final String gn_ql = "hf_gnql_";

	public static final String writeTable = "userlife";

	private static final String hdfs = "hdfs://nn1:9000";
	private static final String hbase = "nn1:60000";
	private static final String zookeeper = "view,nn1,dn1";
	/* 查询定义 **/
	private static final String[] columnQualifiers = { "stime", "etime", "fci", "flac", "ci", "lac" };
	private static final String columnFamily = "info";

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	private static boolean DEBUG = false;

	public static Configuration initConf() {
		Map<String, String> _map = ConfigUtil.resolveConfigProFile("hadoop.properties");
		String _hdfs = ConfigUtil.getConfigValue(_map, "fs.defaultFS", hdfs);
		String _hbase = ConfigUtil.getConfigValue(_map, "hbase.master", hbase);
		String _zookeeper = ConfigUtil.getConfigValue(_map, "hbase.zookeeper.quorum", zookeeper);

		Configuration conf = HBaseConfigUtil.createConfig(_hdfs, _hbase, _zookeeper);
		return conf;
	}

	/**
	 * 
	 * @param args
	 *            传递 参数
	 *            <ol>
	 *            <li>开始时间(20161201)</li>
	 *            <li>结束时间(20161210)</li>
	 *            <li>时间门限(可以为空)</li>
	 *            <li>周数</li>
	 *            <li>月份</li>
	 *            <li>调试</li>
	 *            </ol>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		logout.info("用户 标签 标记 开始---------------------- ");
		Configuration conf = initConf();
		// map reduce 错误 数占总数的 百分比（0-100）
		conf.set("mapred.max.map.failures.percent", "10");
		conf.set("mapred.max.reduce.failures.percent", "10");

		Connection con = null;
		try {
			// 时间
			if (args.length < 2) {
				throw new RuntimeException("需要传递2个 时间参数: yyyyMMdd , 起止时间 ");
			} else {
				logout.info("init Param: start=" + args[0] + " , end=" + args[1]);
			}
			if (args.length >= 3) {
				String s = args[2];
				if (StringUtil.isNum(s)) {
					logout.info("init Param: 更改 时长 门限值= " + s);
					// UserlifeService.TIMERANGE = Integer.valueOf(s);
				}
			}
			if (args.length >= 4) {
				conf.set("week", args[3]);
			}
			if (args.length >= 5) {
				conf.set("month", args[4]);
			}
			if (args.length >= 6) {
				if ("debug".equals(args[5].trim())) {
					DEBUG = true;
					logout.info("启用 DEBUG 模式");
				} else {
					DEBUG = false;
				}
			}

			con = HBaseUtil.getConnection(conf);
			createTable(writeTable, con);
			Date startDt = sdf.parse(args[0]);
			Date endDt = sdf.parse(args[1]);

			List<Date> dates = DateUtil.dateSplit(startDt, endDt, DateUtil.RANGE_DAY);
			List<String> tables = new ArrayList<String>();
			logout.info("时间 间隔：" + dates.size());
			for (Date date : dates) {
				String _cs = cs_ql.concat(sdf.format(date));
				String _gn = gn_ql.concat(sdf.format(date));

				if (HBaseUtil.isTableExists(con, _cs)) {
					tables.add(_cs);
				}
				if (HBaseUtil.isTableExists(con, _gn)) {
					tables.add(_gn);
				}
			}
			for (String tb : tables) {
				logout.info(" 增加表: " + tb);
			}

			conf.set("conf.columnfamily", columnFamily);
			conf.set("writeTable", writeTable);
			// conf.set("timerange", "" + UserlifeService.TIMERANGE);

			Job job = Job.getInstance(conf, "userlife tags");
			job.setJarByClass(UserLifeMain.class);

			// 此句 在发布的时候删除
			// job.setJar("target/localtion_aware_mapreduce-0.0.1-SNAPSHOT.jar");
			TableMapReduceUtil.addDependencyJars(job);
			List<Scan> scans = createScans(tables);
			// 加载多张表的数据
			TableMapReduceUtil.initTableMapperJob(scans, UserLifeMap.class, Text.class, Text.class, job);
			TableMapReduceUtil.initTableReducerJob(writeTable, UserLifeReduce.class, job);
			job.setNumReduceTasks(1);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			logout.info("用户 标签 标记完成 ");
		} finally {
			HBaseUtil.close(con);
		}
	}

	public static List<Scan> createScans(List<String> tables) {
		Scan scan = null;
		List<Scan> scans = new ArrayList<Scan>();
		if (DEBUG) {
			List<String> list = new ArrayList<String>();
			list.add("460075570000000");
			list.add("460007163622789");
			list.add("460000802503270");
			list.add("460000752521392");
			list.add("460000536017263");
			list.add("460000822526660");

			list.add("460008004532642");
			list.add("460006519035283");
			list.add("460078571413342");
			list.add("460078055106540");
			list.add("460078571026298");
			list.add("460028057140516");
			list.add("460006509049936");
			list.add("460006509048257");
			list.add("460006519023401");
			list.add("460006509051573");
			list.add("460006509042905");
			list.add("460006519005492");
			list.add("460078065324141");
			list.add("460006509026250");
			list.add("460006509006296");
			list.add("460006528022120");
			list.add("460006519030137");
			list.add("460006819006927");
			list.add("460006509043399");
			list.add("460006519008462");
			list.add("460006509016038");
			list.add("460005709043138");
			list.add("460006509046868");
			list.add("460006519004354");
			list.add("460008082316718");
			list.add("460006509042721");
			list.add("460006519029422");
			list.add("460008784569403");
			list.add("460006528030155");
			list.add("460006761604783");
			list.add("460006519039835");
			list.add("460006519001188");
			list.add("460078058126327");
			list.add("460007163611604");
			list.add("460006509025770");
			list.add("460008103536185");
			list.add("460005730739639");
			list.add("460005710115341");
			list.add("460007184644620");
			list.add("460078067106099");
			list.add("460006509042197");
			list.add("460008103638340");
			list.add("460006509049243");
			list.add("460005728001756");
			list.add("460006509021571");
			list.add("460005719010601");
			list.add("460006509002756");
			list.add("460007144627817");
			list.add("460007403645969");
			list.add("460006509044787");
			list.add("460006519026243");
			list.add("460006519036294");
			list.add("460006519038845");
			list.add("460006509009442");
			list.add("460006509044814");
			list.add("460008154614204");
			list.add("460006819002862");
			list.add("460007163622789");
			list.add("460008103532458");
			list.add("460007104647807");
			list.add("460007104641066");
			list.add("460006509048075");
			list.add("460006509004722");
			list.add("460006519020709");
			list.add("460006819006427");
			list.add("460078571321639");
			list.add("460006519007164");
			list.add("460006509022435");
			list.add("460005710113559");
			list.add("460005817029181");
			list.add("460005719012099");
			list.add("460006549075074");
			list.add("460006509030808");
			list.add("460078065121056");
			list.add("460006509046740");
			list.add("460006509026608");
			list.add("460005730739431");
			list.add("460000536011966");
			list.add("460006639001188");
			list.add("460006519001391");
			list.add("460078067106166");
			list.add("460005899019852");
			list.add("460006519001581");
			list.add("460007163618276");
			list.add("460006509042107");
			list.add("460006509046389");
			list.add("460006599007867");
			list.add("460006509048902");
			list.add("460006509041236");
			list.add("460005839002995");
			list.add("460078067107341");
			list.add("460005719012630");
			list.add("460005806004478");
			list.add("460006509048169");
			list.add("460006509049357");
			list.add("460005718039061");
			list.add("460006519035283");
			list.add("460008174628992");
			list.add("460008004532642");
			list.add("460008154612539");
			list.add("460000822526660");
			list.add("460008103632390");
			list.add("460008174616908");
			list.add("460005719013199");
			list.add("460006516007689");
			list.add("460008152507175");
			list.add("460006509025452");
			list.add("460006519024137");
			list.add("460006509041801");
			list.add("460005728014316");
			list.add("460006509041334");
			list.add("460007172523925");
			list.add("460005719013423");
			list.add("460006509025680");
			list.add("460006509048337");
			list.add("460006509041352");
			list.add("460007133646076");
			list.add("460007154629445");
			list.add("460005806002948");
			list.add("460008022352711");
			list.add("460006509049298");
			list.add("460006509044211");
			list.add("460006546006076");
			list.add("460008103538333");
			list.add("460005798025180");
			list.add("460008004637543");
			list.add("460005716037560");
			list.add("460005748027684");
			list.add("460005719012603");
			list.add("460008004533101");
			list.add("460006509044749");
			list.add("460000802503270");
			list.add("460006519036451");
			list.add("460008034612391");
			list.add("460005806029837");
			list.add("460006509047114");
			list.add("460006519031311");
			list.add("460008004639215");
			list.add("460005719013265");
			list.add("460005806028780");
			list.add("460006528022787");
			list.add("460028571417127");
			list.add("460007114624143");
			list.add("460007104634870");
			list.add("460006509021930");
			list.add("460008004538383");
			list.add("460006509041342");
			list.add("460005706028727");
			list.add("460005758023037");
			list.add("460007124642720");
			list.add("460005719011581");
			list.add("460006509047786");
			list.add("460006509006294");
			list.add("460008004537487");
			list.add("460008004537423");
			list.add("460006819001692");
			list.add("460005718039441");
			list.add("460008850833516");
			list.add("460007193610071");
			list.add("460005719011525");
			list.add("460006509046089");
			list.add("460006509003634");
			list.add("460005798024795");
			list.add("460006509042561");
			list.add("460008820803364");
			list.add("460005719013690");
			list.add("460008004633493");
			list.add("460006509048818");
			list.add("460007154636629");
			list.add("460005719012637");
			list.add("460078057009162");
			list.add("460006509012339");
			list.add("460006538057705");
			list.add("460078068326208");
			list.add("460006519029422");
			list.add("460009594803393");
			list.add("460078068909812");
			list.add("460079057851802");
			list.add("460078068620685");
			list.add("460009103140351");

			for (String imsi : list) {
				for (String table : tables) {
					String _i = new StringBuffer(imsi).reverse().toString().trim();
					String sk = _i.concat("_0");
					String ek = _i.concat("_9");
					scan = new Scan(sk.getBytes(), ek.getBytes());
					scan.setCacheBlocks(false);
					scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getBytes());
					scans.add(scan);
				}
			}
		} else {
			for (String table : tables) {
				// String _i = new
				// StringBuffer(imsi).reverse().toString().trim();
				// String sk = _i.concat("_0");
				// String ek = _i.concat("_9");
				// scan = new Scan(sk.getBytes(), ek.getBytes());
				scan = new Scan();
				scan.setCaching(500);
				scan.setCacheBlocks(false);
				scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getBytes());
				scans.add(scan);
			}
		}

		return scans;
	}

	public static void createTable(String tableNames, Connection conn)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		TableName userTable = TableName.valueOf(tableNames);
		Admin admin = conn.getAdmin();
		HTableDescriptor tableDescr = new HTableDescriptor(userTable);
		HColumnDescriptor family = new HColumnDescriptor("info".getBytes());
		family.setBloomFilterType(BloomType.ROW);
		family.setDataBlockEncoding(DataBlockEncoding.PREFIX);
		family.setInMemory(true);
		family.setCompressionType(Algorithm.SNAPPY);
		family.setCompactionCompressionType(Algorithm.SNAPPY);
		tableDescr.addFamily(family);
		if (admin.tableExists(userTable)) {
			logout.info(tableNames + " 已存在");
			admin.close();
		} else {
			byte[][] by1 = null;
			by1 = new byte[][] { "0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(), "4".getBytes(),
					"5".getBytes(), "6".getBytes(), "7".getBytes(), "8".getBytes(), "9".getBytes() };
			admin.createTable(tableDescr, by1);
			admin.close();
			logout.info(tableNames + " 已创建");
		}
	}
}
