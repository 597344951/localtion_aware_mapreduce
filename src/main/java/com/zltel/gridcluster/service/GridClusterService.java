package com.zltel.gridcluster.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zltel.common.utils.string.StringUtil;
import com.zltel.gridcluster.bean.GridGroup;
import com.zltel.gridcluster.bean.GridPointer;
import com.zltel.gridcluster.bean.Group;
import com.zltel.location_aware.userlife.bean.Pointer;
import com.zltel.location_aware.userlife.utils.LocalDistince;

public class GridClusterService {
	private static final Logger logout = LoggerFactory.getLogger(GridClusterService.class);
	static {

	}

	/**
	 * 获取实例
	 * 
	 * @return
	 */
	public static GridClusterService getInstince() {
		return new GridClusterService();
	}

	/**
	 * 判断 是否 是邻居<br>
	 * 1. 点必须没有在已有 邻居点钟 <br>
	 * 2. 距离中心点距离不能超过 最大距离
	 * 
	 * @param _ng
	 * @return true 是邻居，false 不是邻居
	 */
	public static boolean checkNeighbor(int x, int y, Group _ng, long neighbor_distince) {
		int _x = Math.abs(_ng.getX() - x);
		int _y = Math.abs(_ng.getY() - y);
		int distince = _x * _x + _y * _y;
		if (distince > neighbor_distince * neighbor_distince) {
			return false;
		}
		return true;
	}

	private Map<String, Group> _group = new HashMap<String, Group>();

	/**
	 * 标记 索引
	 * 
	 * @param pointer
	 */
	public void markIndex(Pointer pointer, int step, int weight) {
		GridPointer gp = new GridPointer(pointer);
		gp.setX(LocalDistince.getIndex(gp.getLat(), step, weight));
		gp.setY(LocalDistince.getIndex(gp.getLng(), step, weight));
		String markkey = com.zltel.gridcluster.bean.Group.markKey(gp.getX(), gp.getY());
		com.zltel.gridcluster.bean.Group group = _group.get(markkey);
		if (group == null) {
			group = new Group();
			group.setX(gp.getX());
			group.setY(gp.getY());
		}
		group.addGridPointer(gp);
		_group.put(markkey, group);
	}

	/**
	 * 获取 组以及相邻的组，并按总分数 高低排序
	 * 
	 * @param score_threshold
	 *            分组 分数门限值
	 * @return
	 */
	public List<GridGroup> makeGroup(long score_threshold, long neighbor_distince) {
		filterGroup(score_threshold);
		Set<GridGroup> retList = new HashSet<GridGroup>();
		for (Map.Entry<String, Group> entry : _group.entrySet()) {
			Group g = entry.getValue();
			GridGroup gg = new GridGroup();
			gg.setX(g.getX());
			gg.setY(g.getY());

			gg.addNeighbor(g);
			neighbers(gg, g, neighbor_distince);
			gg.calcScore();
			retList.add(gg);
		}
		List<GridGroup> ret = new ArrayList<GridGroup>(retList);
		Collections.sort(ret, getGridGroupCompareByScore());
		return ret;
	}

	/**
	 * 过滤 掉 比重低的点
	 */
	public void filterGroup(long score_threshold) {
		List<String> rmk = new ArrayList<String>();
		for (Map.Entry<String, Group> entry : _group.entrySet()) {
			Group g = entry.getValue();
			if (g.calcScore() < score_threshold) {
				rmk.add(entry.getKey());
			}
		}
		logout.info("过滤门限:" + score_threshold + "  ,过滤个数:" + rmk.size());
		for (String k : rmk) {
			_group.remove(k);
		}
	}

	/**
	 * 递归遍历 邻居
	 * 
	 * @param gg
	 * @param g
	 */
	public void neighbers(GridGroup gg, Group _g, long neighbor_distince) {
		if (_g == null) {
			return;
		}
		int x = _g.getX();
		int y = _g.getY();
		String _mk = null;
		Group _ng = null;
		// x 不变， y +/- 1
		_mk = Group.markKey(x, y + 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
		_mk = Group.markKey(x, y - 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
		// y 不变，x +/- 1

		_mk = Group.markKey(x + 1, y);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
		_mk = Group.markKey(x - 1, y);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}

		// x-1,y +/- 1
		_mk = Group.markKey(x - 1, y + 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
		_mk = Group.markKey(x - 1, y - 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}

		// x+1,y +/- 1
		_mk = Group.markKey(x + 1, y + 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
		_mk = Group.markKey(x + 1, y - 1);
		_ng = _group.get(_mk);
		if (_ng != null) {
			if (gg.checkNeighbor(_ng, neighbor_distince)) {
				gg.addNeighbor(_ng);
				neighbers(gg, _ng, neighbor_distince);
			}
		}
	}

	public Comparator<GridGroup> getGridGroupCompareByScore() {
		return new Comparator<GridGroup>() {

			public int compare(GridGroup o1, GridGroup o2) {
				long score1 = o1.calcScore();
				long score2 = o2.calcScore();
				return Long.compare(score2, score1);
			}

		};
	}

	public void clear() {
		_group.clear();
	}

	public StringBuffer print(GridGroup gg) {
		Set<Group> neighbors = gg.getNeighbors();
		int minX = Integer.MAX_VALUE, maxX = 0, minY = Integer.MAX_VALUE, maxY = 0;
		Map<String, Long> scoreMap = new HashMap<String, Long>();
		for (Group g : neighbors) {
			int _x = g.getX();
			int _y = g.getY();
			minX = minX > _x ? _x : minX;
			maxX = maxX > _x ? maxX : _x;
			minY = minY > _y ? _y : minY;
			maxY = maxY > _y ? maxY : _y;
			scoreMap.put(Group.markKey(_x, _y), g.getScore());
		}
		StringBuffer sb = new StringBuffer();
		// sb.append(minY).append("-" + maxY + "\n");

		StringBuffer xhead = new StringBuffer();
		for (int x = minX; x <= maxX; x++) {
			xhead.append(" | ");
			xhead.append(x - minX);
		}
		xhead.append("\n");
		int _xhl = xhead.length();
		for (int i = 0; i < _xhl; i++) {
			xhead.append("- ");
		}
		sb.append("\n");
		int yVwidth = 0;
		for (int y = minY; y <= maxY; y++) {
			yVwidth = String.valueOf(y).length();
			sb.append(y);
			sb.append(" ");
			for (int x = minX; x <= maxX; x++) {

				sb.append("|");
				String k = Group.markKey(x, y);
				Long score = scoreMap.get(k);
				String dis = "";
				if (score == null) {
					score = 0l;
					dis = "-";
				} else {
					dis = "1";
				}
				if (gg.getX() == x && gg.getY() == y) {
					dis = "<" + dis + ">";
				} else {
					dis = " " + dis + " ";
				}
				sb.append(dis);
			}
			sb.append("\n");
		}
		xhead.insert(0, StringUtil.toNumFix(minX, yVwidth));
		sb.insert(0, xhead.toString());
		sb.insert(0, "分组 矩阵信息，分数:" + gg.getScore() + "  x:" + gg.getX() + ",y:" + gg.getY() + "\n");
		sb.append("\n");

		// System.out.println(sb.toString());
		return sb;
	}

	public String printGroups(List<GridGroup> ggs) {
		int minX = Integer.MAX_VALUE, maxX = 0, minY = Integer.MAX_VALUE, maxY = 0;
		Map<String, Long> scoreMap = new HashMap<String, Long>();
		int size = 0;
		for (GridGroup g : ggs) {
			int _x = g.getX();
			int _y = g.getY();
			minX = minX > _x ? _x : minX;
			maxX = maxX > _x ? maxX : _x;
			minY = minY > _y ? _y : minY;
			maxY = maxY > _y ? maxY : _y;
			scoreMap.put(Group.markKey(_x, _y), g.getScore());
			size++;
			if (size >= 1) {
				break;
			}
		}
		StringBuffer sb = new StringBuffer();
		sb.append("----分组 矩阵信息 (" + ggs.size() + ")----");
		sb.append("\n");
		sb.append(" x-------------------------------------------->\n");
		sb.append("y\n");
		boolean isf = true;
		for (int y = minY; y <= maxY; y++) {
			sb.append("");
			if (isf) {
				for (int x = minX; x <= maxX; x++) {
					sb.append(" | ");
					sb.append(x);
				}
				sb.append("\n");
				isf = false;
			}
			sb.append(y);
			for (int x = minX; x <= maxX; x++) {

				sb.append(" | ");
				String k = Group.markKey(x, y);
				Long score = scoreMap.get(k);
				if (score == null) {
					score = 0l;
				} else {
					score = 1l;
				}
				sb.append(score);
			}
			sb.append("\n");
		}

		System.out.println(sb.toString());
		return sb.toString();
	}

}
