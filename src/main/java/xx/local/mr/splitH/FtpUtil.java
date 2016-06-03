/**
 * Project Name:zjmonitorDataExport
 * File Name:FtpUtil.java
 * Package Name:com.zltel.data
 * Date:2016-1-28����6:23:26
 * Copyright (c) 2016, Eastcom,Inc.All Rights Reserved.
 *
 */

package xx.local.mr.splitH;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

/**
 * ClassName:FtpUtil <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016-1-28 ����6:23:26 <br/>
 *
 * @author chenxiao
 * @version 1.0.0
 * @since JDK 1.7
 * @see
 */
public class FtpUtil {

  public boolean uploadFile(String filename, InputStream input, String path,
      String tpath) {
    String url = "10.211.191.139";
    int port = 21;
    String username = "zltel";
    String password = "zl3edc$RFV";
    boolean success = false;
    FTPClient ftp = new FTPClient();
    try {
      int reply;
      ftp.connect(url, port);
      ftp.login(username, password);
      reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        return success;
      }
      FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);
      conf.setServerLanguageCode("zh");
      ftp.setFileType(FTP.BINARY_FILE_TYPE);
      ftp.setControlEncoding("UTF-8");
      if (path != null) {
        ftp.makeDirectory(path);
      }
      if (!ftp.changeWorkingDirectory(tpath)) {
        ftp.makeDirectory(tpath);
        ftp.changeWorkingDirectory(tpath);
      } else {

      }
      
      ftp.storeFile(filename, input);
      input.close();
      ftp.logout();
      success = true;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch (IOException ioe) {
        }
      }
    }
    return success;
  }

  public boolean rename(String name, String remote, String path)
      throws IOException {
    String url = "10.211.191.139";
    int port = 21;
    String username = "zltel";
    String password = "zl3edc$RFV";
    boolean success = false;
    FTPClient ftp = new FTPClient();
    try {
      int reply;
      ftp.connect(url, port);
      ftp.login(username, password);
      reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        return success;
      }
      ftp.enterLocalPassiveMode();
      ftp.setFileType(FTP.BINARY_FILE_TYPE);
      if (!ftp.changeWorkingDirectory(path)) {
        ftp.makeDirectory(path);
        ftp.changeWorkingDirectory(path);
      }

      FTPFile[] files = ftp.listFiles(remote);
      if (files.length == 1) {
        success = ftp.rename(remote, name);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch (IOException ioe) {
        }
      }
    }
    return success;
  }

  // public static void main(String[] args) {
  // String ss="hello word";
  // uploadFile("dd.txt",new ByteArrayInputStream(ss.getBytes()));
  // }
}
