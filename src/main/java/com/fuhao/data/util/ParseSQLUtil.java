package com.fuhao.data.util;

import java.util.Collection;
import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * 本类的功能是根据所给的json数据和表名称，解析为DML语句
 * 
 * @author illin
 *
 */
public class ParseSQLUtil {
	/**
	 * 将给出的字符串尾部的逗号去掉
	 * 
	 * @param oldStr
	 *            需要修改的字符串
	 * @return 返回一个不是以逗号结尾的字符串
	 */
	private static StringBuffer deleteEnd(StringBuffer oldStr) {
		int len = oldStr.length();
		if (oldStr.lastIndexOf(",") == len - 1) {
			oldStr = new StringBuffer(oldStr.substring(0, len - 1));
		}
		return oldStr;
	}

	/**
	 * 将传入的json和tableName解析为SQL数据库的插入语句。
	 * 
	 * @param json
	 *            要插入的数据
	 * @param tableName
	 *            表名称
	 * @return {@link String}返回插入语句
	 */
	public static String parseToInsertSQL(JSONObject json, String tableName) {
		Iterator keys = json.keys();
		StringBuffer insertSQL = new StringBuffer();
		insertSQL.append("INSERT INTO ").append(tableName.toUpperCase())
				.append("(");
		StringBuffer placeHolder = new StringBuffer();
		while (keys.hasNext()) {
			Object key = keys.next();
			insertSQL.append(key + ",");
			placeHolder.append("?,");
		}
		insertSQL = deleteEnd(insertSQL);
		insertSQL.append(") VALUES(").append(deleteEnd(placeHolder))
				.append(");");
		return insertSQL.toString();
	}

	/**
	 * 将传入的json、condition、tableName解析为SQL数据库的更新语句。
	 * 
	 * @param json
	 * @param condition
	 * @param tableName
	 * @return
	 */
	public static String parseToUpdateSQL(JSONObject json, String condition,
			String tableName) {
		Iterator keys = json.keys();
		StringBuffer updateSQL = new StringBuffer();
		updateSQL.append("UPDATE ").append(tableName.toUpperCase())
				.append(" SET ");
		while (keys.hasNext()) {
			Object key = keys.next();
			updateSQL.append(key + "=?,");
		}
		updateSQL = deleteEnd(updateSQL);
		updateSQL.append(" " + condition);
		return updateSQL.toString();
	}

	public static String parseToQuerySQL(JSONArray fields, String condition,
			String tableName) {
		int len=fields.size();
		StringBuffer querySQL=new StringBuffer("SELECT ");
		for (int i = 0; i <len; i++) {
			querySQL.append(fields.get(i)+",");
		}
		querySQL=deleteEnd(querySQL);
		querySQL.append(" FROM ").append(tableName.toUpperCase()).append(" ").append(condition);
		
		return querySQL.toString();
	}

	public static String parseToDeleteSQL(String condition, String tableName) {
		StringBuffer deleteSQL=new StringBuffer();
		deleteSQL.append("DELETE FROM ").append(tableName).append(" ").append(condition);
		return deleteSQL.toString();
	}
}
