package com.zltel.location_aware.userlife.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.zltel.common.utils.string.StringUtil;

public class LogUtil {

	public final static Integer DEBUG = 0;
	public final static Integer INFO = 1;
	public final static Integer NOTICE = 2;
	public final static Integer ERROR = 3;
	public final static Integer FATAL = 3;

	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// private final static SimpleDateFormat sdf_date = new
	// SimpleDateFormat("yyyy-MM-dd");

	private static int currentLevel = LogUtil.DEBUG;

	private static boolean LOCALDebug = false;

	public static void debug(String text, String outpath, Configuration config) {
		if (DEBUG >= currentLevel) {
			String m = getBaseStr() + " DEBUG   " + text;
			if (LOCALDebug) {
				System.out.println(m);
			} else {
				try {
					write(m, outpath, config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void info(String msg, String outpath, Configuration config) {
		if (INFO >= currentLevel) {
			String m = getBaseStr() + " INFO   " + msg;
			if (LOCALDebug) {
				System.out.println(m);
			} else {
				try {
					write(m, outpath, config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void notice(String msg, String outpath, Configuration config) {
		if (NOTICE >= currentLevel) {
			String m = getBaseStr() + " NOTICE  " + msg;
			if (LOCALDebug) {
				System.out.println(m);
			} else {
				try {
					write(m, outpath, config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void error(String msg, String outpath, Configuration config) {
		if (ERROR >= currentLevel) {
			String m = getBaseStr() + " ERROR  " + msg;
			if (LOCALDebug) {
				System.err.println(m);
			} else {
				try {
					write(m, outpath, config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		//

	}

	public static void error(String msg, String outpath, Configuration config, Exception e) {
		error(msg + readException(e), outpath, config);
	}

	public static void error(String outpath, Configuration config, Exception e) {
		error(readException(e), outpath, config);
	}

	public static void fatal(String msg, String outpath, Configuration config) {
		if (FATAL >= currentLevel) {
			String m = getBaseStr() + " ERROR  " + msg;
			if (LOCALDebug) {
				System.err.println(m);
			} else {
				try {
					write(m, outpath, config);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void fatal(String msg, String outpath, Configuration config, Exception e) {
		fatal(msg + readException(e), outpath, config);
	}

	public static void fatal(String outpath, Configuration config, Exception e) {
		fatal(readException(e), outpath, config);
	}

	public static String readException(Throwable e) {
		if (e == null)
			return null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			e.printStackTrace(new PrintStream(baos));
		} catch (Exception e2) {
			e2.printStackTrace();
		} finally {
			try {
				baos.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return baos.toString();
	}

	private static synchronized String getBaseStr() {
		StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
		boolean find = false;
		StackTraceElement target_s = null;
		if (stacks.length <= 3) {
			for (StackTraceElement ste : stacks) {
				String fn = ste.getFileName();
				if (find) {
					target_s = ste;
					break;
				}
				if (fn.equals(LogUtil.class.getSimpleName() + ".java")) {
					find = true;
				}
			}
		} else {
			target_s = stacks[3];
		}
		return sdf.format(new Date()) + " (" + target_s.getFileName() + ":" + target_s.getLineNumber() + ") ";
	}

	/**
	 * 获取currentLevel
	 * 
	 * @return currentLevel currentLevel
	 */
	public final static int getCurrentLevel() {
		return currentLevel;
	}

	/**
	 * 设置currentLevel
	 * 
	 * @param currentLevel
	 *            currentLevel
	 */
	public final static void setCurrentLevel(int currentLevel) {
		LogUtil.currentLevel = currentLevel;
	}

	private static void write(String text, String outpath, Configuration config) {
		if (StringUtil.isNullOrEmpty(text, outpath) || config == null) {
			return;
		}
		try {
			FileSystem fs = FileSystem.get(URI.create(outpath), config);
			Path op = new Path(outpath);
			OutputStream out = null;
			if (fs.exists(op)) {
				out = fs.append(op);
			} else {
				// 创建文件
				out = fs.create(op);
			}
			try {
				if (out != null) {
					out.write(text.concat("\r\n").getBytes());
					out.flush();
				}
			} finally {
				try {
					if (out != null)
						out.close();
				} catch (Exception e) {
					// logout.error("", e);
				}
			}
		} catch (Exception e) {
			// logout.error("", e);
		}
	}

	public static void main(String[] args) {
		System.out.println(getBaseStr());
	}

}
