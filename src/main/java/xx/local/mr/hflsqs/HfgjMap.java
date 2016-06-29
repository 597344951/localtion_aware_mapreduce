package xx.local.mr.hflsqs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HfgjMap extends Mapper<LongWritable, Text, Text, Text> {

	private Map<String, String> fgsciMAP = new HashMap<String, String>();

	public enum Counters {
		LINES
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			fgsciMAP.put("10000", "10000");
			fgsciMAP.put("10010", "10010");
			fgsciMAP.put("95079", "95079");
			fgsciMAP.put("96171", "96171");
			fgsciMAP.put("0571-10000", "0571-10000");
			fgsciMAP.put("0571-10010", "0571-10010");
			fgsciMAP.put("0571-95079", "0571-95079");
			fgsciMAP.put("0571-96171", "0571-96171");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().trim().split("\\t");
		String[] colum = null;
		colum = strs[1].trim().split(",");
		if (colum.length > 15) {
			String cnum = colum[2];
			String cdnum = colum[3];
			String st = colum[6];
			if (fgsciMAP.get(cnum) != null || fgsciMAP.get(cdnum) != null) {
				context.write(new Text(cnum + "_" + cdnum), new Text(st));

			}
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
