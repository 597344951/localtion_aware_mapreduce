package com.zltel.location_aware.ci_threshold.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * CI小区统计值
 * 
 * @author Wangch
 *
 */
public class CiCount implements WritableComparable<CiCount> {
	public static final int NET_TYPE_2 = 2;
	public static final int NET_TYPE_3 = 3;
	public static final int NET_TYPE_4 = 4;
	/** 小区id **/
	public String ci;

	/** 上传流量 **/
	public long up_bytes;
	/** 下载流量 **/
	public long down_bytes;
	/** 用户统计 **/
	public long userCount;
	/** 网络类型 **/
	public int netType;

	public void write(DataOutput out) throws IOException {
		out.writeUTF(ci);
		out.writeLong(up_bytes);
		out.writeLong(down_bytes);
		out.writeLong(userCount);
		out.writeInt(netType);
	}

	public void readFields(DataInput in) throws IOException {
		this.ci = in.readUTF();
		this.up_bytes = in.readLong();
		this.down_bytes = in.readLong();
		this.userCount = in.readLong();
		this.netType = in.readInt();
	}

	public int compareTo(CiCount o) {
		if (ci != null) {
			return ci.compareTo(o.ci);
		} else {
			return 0;
		}
	}

	/**
	 * @return the up_bytes
	 */
	public long getUp_bytes() {
		return up_bytes;
	}

	/**
	 * @param up_bytes
	 *            the up_bytes to set
	 */
	public void setUp_bytes(long up_bytes) {
		this.up_bytes = up_bytes;
	}

	/**
	 * @return the down_bytes
	 */
	public long getDown_bytes() {
		return down_bytes;
	}

	/**
	 * @param down_bytes
	 *            the down_bytes to set
	 */
	public void setDown_bytes(long down_bytes) {
		this.down_bytes = down_bytes;
	}

	/**
	 * @return the userCount
	 */
	public long getUserCount() {
		return userCount;
	}

	/**
	 * @param userCount
	 *            the userCount to set
	 */
	public void setUserCount(long userCount) {
		this.userCount = userCount;
	}

	/**
	 * @return the netType
	 */
	public int getNetType() {
		return netType;
	}

	/**
	 * @param netType
	 *            the netType to set
	 */
	public void setNetType(int netType) {
		this.netType = netType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CiCount [ci=");
		builder.append(ci);
		builder.append(", up_bytes=");
		builder.append(up_bytes);
		builder.append(", down_bytes=");
		builder.append(down_bytes);
		builder.append(", userCount=");
		builder.append(userCount);
		builder.append(", netType=");
		builder.append(netType);
		builder.append("]");
		return builder.toString();
	}

}
