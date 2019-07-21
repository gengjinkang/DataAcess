package com.fuhao.data.column;

import java.util.Map;

public interface ColumnManager {

	/**
	 * 获取字段信息的方法
	 * 
	 * @param column
	 * @return
	 */
	Map<String, String> getInfo(String column);

	/**
	 * 获取数据类型
	 * 
	 * @param column
	 * @return
	 */
	String getType(String column);

	/**
	 * 获取当前字段所在的表名称
	 * 
	 * @param column
	 * @return
	 */
	String getTableName(String column);

	/**
	 * 获取当前字段所使用的数据源的信息
	 * @param column
	 * @return
	 */
	String getSourceName(String column);
	/**
	 * 获取
	 * @param column
	 * @return
	 */
	String getSchema(String column);

	String get(String column, String info);
	/**
	 * 保存字段的元信息到数据库中
	 * @param columnInfo
	 */
	void saveAllInfo(Map<String, Map<String, String>> columnInfo);
}
