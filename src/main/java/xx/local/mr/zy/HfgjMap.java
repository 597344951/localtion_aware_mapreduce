package xx.local.mr.zy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HfgjMap extends Mapper<LongWritable, Text, Text, Text> {
	// private Map<String, String> xzqci = new HashMap<String, String>();
	private Map<String, String> fgsciMAP = new HashMap<String, String>();

	public enum Counters {
		LINES
	}

	// private Map<String, String> userMap = new HashMap<String, String>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = null;
		FileSystem fs = null;
		try {
			conf = new Configuration();
			fs = FileSystem.get(conf);
			BufferedReader brt = new BufferedReader(new InputStreamReader(
					fs.open(new Path("/user/zltel/hfqs/conf/xzqfgsci.txt")),
					"UTF-8"));
			String line = null;
			while ((line = brt.readLine()) != null) {
				if (line != null && line.length() > 0) {
					String[] strs = line.trim().split(",");
					if (strs.length == 4) {
						// String xzqbh = strs[1].trim();
						String fgsbh = strs[2].trim();
						String ci = strs[3].trim();
						if (fgsbh.matches("\\d+")) {
							if ("1002".equals(fgsbh)) {
								fgsciMAP.put(ci, fgsbh);
							}
						}
						// if (fgsciMAP.size() > 500) {
						//
						// }
					}
				}
			}
			brt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String twci = null;
		String trci = null;
		String fci = null;
		String cnum = null;
		String[] colum = null;
		String line = value.toString();
		String[] tmp = line.split("\\t");
		String lastci = null;
		String type = null;
		String rat = null;
		String imsi = null;

		colum = (tmp[1].trim() + "1").split("\\|");
		try {

			cnum = colum[2];
			imsi = colum[4];
			rat = colum[7];
			if (cnum.startsWith("86")) {
				cnum = cnum.substring(2, cnum.length());
			}

			if (cnum.startsWith("1")) {
			} else {
				if (imsi != null && !imsi.startsWith("460")) {

				} else {
					return;
				}
			}

			twci = Long.parseLong("".equals(colum[11]) ? "0" : colum[11], 16)
					+ "";
			trci = Long.parseLong("".equals(colum[12]) ? "0" : colum[12], 16)
					+ "";

			String ffci = colum[colum.length - 2];
			if (ffci != null && ffci.length() > 2) {
				fci = Long.parseLong(ffci, 16) + "";
			}
			if ("6".equals(rat)) {
				lastci = fci;
				type = "4";
			} else if ("2".equals(rat)) {
				lastci = twci;
				type = "2";
			} else if ("1".equals(rat)) {
				lastci = trci;
				type = "3";
			}

			if (fgsciMAP.get(lastci) != null) {
				context.getCounter(Counters.LINES).increment(1);
				String rkey = cnum + "_" + type;

				context.write(new Text(rkey), new Text(cnum + "," + imsi + ","
						+ type + "," + lastci));
			}

		} catch (Exception e) {
			e.printStackTrace();

		}
	}

}
// public void map(LongWritable key, Text value,
// OutputCollector<Text, Text> output, Reporter reporter)
// throws IOException {
// SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
// String line = value.toString() + "1";
// String[] tmp = line.split("\\|");
// String rkey = null;
// StringBuffer cimsi = new StringBuffer();
// // StringBuffer dimsi = new StringBuffer();
// // String cnum = null;
// // String dnum = null;
// String stime = null;
// // String lac_ = null;
// // String flac_ = null;
// if (tmp.length < 10) {
// // tmp = line.split(",");
// // if (tmp.length > 10) {
// // cimsi.append(tmp[0]);
// // dimsi.append(tmp[1]);
// // cnum = tmp[2];
// // dnum = tmp[3];
// // stime = tmp[6];
// // // lac_ = tmp[8];
// // // cdr_id = tmp[12];
// // // call_type = tmp[13];
// // if (cnum.startsWith("86")) {
// // cnum = cnum.substring("86".length(), cnum.length());
// // }
// // if (dnum.startsWith("86")) {
// // dnum = dnum.substring("86".length(), dnum.length());
// // }
// // if ((cnum != null && numMap.get(cnum) != null)
// // || (cimsi.length() > 0 && imsiMap.get(cimsi.toString()) != null))
// // {
// // rkey = cimsi.reverse() + "_" + stime;
// // output.collect(new Text(rkey), value);
// // }
// // if ((dnum != null && numMap.get(dnum) != null)
// // || (dimsi.length() > 0 && imsiMap.get(dimsi.toString()) != null))
// // {
// // rkey = dimsi.reverse() + "_" + stime;
// // output.collect(new Text(rkey), value);
// // }
// // }
// } else {
// // cnum = tmp[2];
// stime = sdf.format(new Date(Long.parseLong(tmp[0]) * 1000));
// cimsi.append(tmp[4]);
// /*
// * lac_ = tmp[10]; flac_ = tmp[tmp.length - 2];
// */
// String rat = tmp[7];
// String imsi = tmp[4];
// rkey = cimsi.reverse() + "_" + stime;
// if ((rat != null && "1".equals(rat)) || imsi == null
// || "".equals(imsi)) {
// output.collect(new Text(rkey), new Text(value));
// }
// // String fci = null;
// // String ffci = tmp[tmp.length - 2];
// // if (ffci != null && ffci.length() > 2) {
// // fci = Long.parseLong(ffci, 16) + "";
// // }
// // if ((cimsi.reverse() == null || "".equals(cimsi.reverse()))) {
// // // && (fci != null && fci.length() > 0 && ciMap.get(fci) !=
// // // null)) {
// // output.collect(new Text(rkey), new Text(value));
// // }
//
// // if ((lac_.length() > 0 && lacf.get(lac_) !=
// // null)||(flac_.length() > 0 && lacf.get(flac_) != null)) {
// // output.collect(new Text(rkey), new Text(value));
// // }
// }

// }
// }
