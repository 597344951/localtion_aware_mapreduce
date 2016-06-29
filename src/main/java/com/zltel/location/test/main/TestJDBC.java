package com.zltel.location.test.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TestJDBC {

	public static void main(String[] args) {
		Connection con = null;// 创建一个数据库连接

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");// 加载Oracle驱动程序
			System.out.println("开始尝试连接数据库！");
			String url = "jdbc:oracle:" + "thin:@192.168.1.109:1521:orcl";// 127.0.0.1是本机地址，XE是精简版Oracle的默认数据库名
			String user = "zjmonitorv2_new";// 用户名,系统默认的账户名
			String password = "zjmonitorv2_new";// 你安装时选设置的密码
			con = DriverManager.getConnection(url, user, password);// 获取连接
			FileReader f = null;
			BufferedReader buffer = null;
			String filename = "D:\\mydata\\xzqfgsci.txt";
			Map<String, String> fgsciMAP = new HashMap<String, String>();
			try {
				f = new FileReader(filename);
				buffer = new BufferedReader(f);
				String s = null;
				StringBuffer sb = new StringBuffer();

				while ((s = buffer.readLine()) != null) {
					String[] strs = s.trim().split(",");
					if (strs.length == 4) {
						String fgsbh = strs[1].trim();
						String ci = strs[3].trim();
						if (fgsbh.matches("\\d+")) {
							if ("2".equals(fgsbh)) {
								fgsciMAP.put(ci, fgsbh);
							}
						}

					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					f.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			FileReader f1 = null;
			BufferedReader buffer1 = null;
			String filename1 = "D:\\mydata\\http0530.txt";
			try {
				StringBuffer sql = new StringBuffer();
				sql.append("insert into hw1 (IMSI,TIME,TYPE,LAC,CI) values (?,?,?,?,?)");
				f1 = new FileReader(filename1);
				buffer1 = new BufferedReader(f1);
				String s1 = null;
				con.setAutoCommit(false);
				PreparedStatement pst = (PreparedStatement) con
						.prepareStatement(sql.toString());
				int count = 0;

				while ((s1 = buffer1.readLine()) != null) {
					String[] strs = ((s1.trim()) + "|1").split("\\|");
					if (strs.length == 7) {
						pst.setString(1, strs[0]);
						pst.setString(2, strs[1]);
						pst.setString(3, strs[2]);
						if (!"".equals(strs[3])) {
							pst.setString(4, strs[3]);
						}
						if (!"".equals(strs[4])) {
							pst.setString(4, strs[4]);
						}

						pst.setString(5, strs[5]);
						pst.addBatch();
						count++;
					}

					if (count % 100000 == 0) {
						pst.executeBatch();
						con.commit();
					}

				}
				// 最后插入不足1w条的数据
				pst.executeBatch();
				con.commit();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					f.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
