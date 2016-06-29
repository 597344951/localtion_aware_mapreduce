package xx.local.mr.hflsqs;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HfgjTask {
	public static void main(String[] args) throws Exception {
		String startDate = null;
		String endDate = null;
		if (args.length == 2) {
			startDate = args[0];
			endDate = args[1];
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
		if (startDate != null && endDate != null) {
			long nlt = sdfh.parse(sdfh.format(new Date())).getTime();
			long olt = sdfh.parse(startDate).getTime();
			long elt = sdfh.parse(endDate).getTime();
			Configuration conf = new Configuration();
			conf.set("mapred.max.map.failures.percent", "100");
			conf.set("mapred.max.reduce.failures.percent", "100");
			Job job = Job.getInstance(conf, "hfCsData");
			job.setJarByClass(HfgjTask.class);
			job.setMapperClass(HfgjMap.class);
			job.setReducerClass(HfgjReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(1);
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

			while (nlt >= olt && olt <= elt) {
				String dated = sdf.format(new Date(olt));
				Calendar c = Calendar.getInstance();
				c.setTime(new Date(olt));
				int h = c.get(Calendar.HOUR_OF_DAY);
				olt += 60 * 60 * 1000;

				// String hfOutGNPath = "/user/zltel/hfOutGN/"
				// + dated.replaceAll("-", "") + "/"
				// + String.format("%02d", h);
				String hfdataOutCS = "/user/zltel/hfdataOutCS/"
						+ dated.replaceAll("-", "") + "/"
						+ String.format("%02d", h);
				System.out.println(hfdataOutCS);
				FileInputFormat.addInputPath(job, new Path(hfdataOutCS));
				// FileInputFormat.addInputPath(job, new Path(hfOutGNPath));

			}
			FileOutputFormat.setOutputPath(job, new Path("/user/zltel/hf_hm/"));
			try {
				job.waitForCompletion(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
