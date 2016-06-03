package xx.local.mr.splitH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HfqsMap extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, String> lacf = new HashMap<String, String>();
	private Map<String, String> lac = new HashMap<String, String>();

	public enum Counters {
		LINES
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String line = value.toString() + "1";
		String[] tmp = line.split("\\|");
		String rkey = null;
		StringBuffer cimsi = new StringBuffer();
		StringBuffer dimsi = new StringBuffer();
		String cnum = null;
		String dnum = null;
		String stime = null;
		String lac_ = null;
		String flac_ = null;
		String call_type = null;
		String cdr_id = null;

		if (tmp.length < 10) {
			tmp = line.split(",");
			if (tmp.length > 13) {

				cimsi.append(tmp[0]);
				dimsi.append(tmp[1]);
				cnum = tmp[2];
				dnum = tmp[3];
				stime = tmp[6];
				lac_ = tmp[8];
				cdr_id = tmp[12];
				call_type = tmp[13];
				if ("0".equals(cdr_id) || "1".equals(cdr_id)) {
					if ("0".equals(call_type) || "2".equals(call_type)
							|| "3".equals(call_type) || "4".equals(call_type)
							|| "5".equals(call_type)) {
						if (cimsi.length() > 0 && cnum.length() > 0
								&& lac.get(lac_) != null) {
							rkey = cimsi.reverse() + "_" + stime;
							context.write(new Text(rkey), value);
						}
					} else if ("1".equals(call_type) || "6".equals(call_type)) {
						if (dimsi.length() > 0 && dnum.length() > 0
								&& lac.get(lac_) != null) {
							rkey = dimsi.reverse() + "_" + stime;
							context.write(new Text(rkey), value);
						}
					}
				} else if ("2".equals(cdr_id) || "3".equals(cdr_id)) {
					if (cimsi.length() > 0 && cnum.length() > 0
							&& lac.get(lac_) != null) {
						rkey = cimsi.reverse() + "_" + stime;
						context.write(new Text(rkey), value);
					}
				} else if ("4".equals(cdr_id) || "5".equals(cdr_id)) {
					if ("0".equals(call_type) || "3".equals(call_type)) {
						if (cimsi.length() > 0 && cnum.length() > 0
								&& lac.get(lac_) != null) {
							rkey = cimsi.reverse() + "_" + stime;
							context.write(new Text(rkey), value);
						}
					} else if ("1".equals(call_type) || "2".equals(call_type)
							|| "4".equals(call_type)) {
						if (dimsi.length() > 0 && dnum.length() > 0
								&& lac.get(lac_) != null) {
							rkey = dimsi.reverse() + "_" + stime;
							context.write(new Text(rkey), value);
						}
					}
				}
			}
		} else {
			cnum = tmp[2];
			stime = sdf.format(new Date(Long.parseLong(tmp[0]) * 1000));
			cimsi.append(tmp[4]);
			lac_ = "".equals(tmp[10]) ? "0" : tmp[10];
			flac_ = "".equals(tmp[tmp.length - 3]) ? "0" : tmp[tmp.length - 3];
			rkey = cimsi.reverse() + "_" + stime;
			if (cimsi != null
					&& cimsi.length() > 0
					&& ((lac_.length() > 0 && lacf.get(lac_) != null) || (flac_
							.length() > 0 && lacf.get(flac_) != null))) {
				context.write(new Text(rkey), new Text(value));

			}
		}

	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		BufferedReader brt = new BufferedReader(new InputStreamReader(
				HfqsMap.class.getResourceAsStream("lac.txt")));
		String line = null;
		try {
			while ((line = brt.readLine()) != null) {
				if (line != null && line.length() > 0) {
					lac.put(line.trim(), line.trim());
					lacf.put(Long.toHexString(Long.parseLong(line.trim()))
							.toUpperCase(),
							Long.toHexString(Long.parseLong(line.trim()))
									.toUpperCase());
				}
			}
			brt.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
