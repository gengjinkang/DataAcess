package com.fuhao.data.datasource.api;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import com.fuhao.data.column.ColumnManager;
import com.fuhao.data.config.Configurations;
import com.fuhao.data.datasource.DataSource;
import com.fuhao.data.fhbase.FuhaoHbase;
import com.fuhao.data.fhbase.HBaseClient;
import com.fuhao.data.util.PhoenixSqlUtil;

public class HbaseDataSource implements DataSource {

	private FuhaoHbase hbase;
	private static final Logger LOG = Logger.getLogger(HbaseDataSource.class);
	private final String url;//
	private final String tableName;
	private static final String SOURCENAME = "HBASE";
	private ColumnManager columnManager;
	private final String KEY = "_key";
	private final String family;//
	private CuratorFramework curatorFramework;

	static {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public HbaseDataSource() {
		this.url = Configurations.PROPERTIES.getProperty("phoenix.url");
		this.tableName = Configurations.PROPERTIES
				.getProperty("hbase_tableName");
		this.family = Configurations.PROPERTIES.getProperty("hbase_family");
		this.hbase = new HBaseClient();
		this.curatorFramework = CuratorFrameworkFactory.builder()
				.connectString("hadoop,slave2,slave3:2181")
				.retryPolicy(new ExponentialBackoffRetry(100, 3)).build();
		LOG.info("Init:{ url:" + url + ",tableName:" + tableName
				+ ",sourceName:" + SOURCENAME + "}");
		this.curatorFramework.start();
	}

	@Override
	public void beginTransaction() {

	}

	@Override
	public String getSourceName() {
		return SOURCENAME;
	}

	@Override
	public Map<String, Map<String, String>> getInfo() {

		Connection connection = null;
		Map<String, Map<String, String>> map = new HashMap<>();
		try {
			connection = DriverManager.getConnection(this.url);
			DatabaseMetaData metaData = connection.getMetaData();

			ResultSet columns = metaData.getColumns(null, null, "", "%");
			int columnCount = columns.getMetaData().getColumnCount();// 获取列的数量
			while (columns.next()) {

				String column = columns.getString("COLUMN_NAME");
				Map<String, String> result = new HashMap<>(columnCount);
				result.put("SOURCENAME", SOURCENAME);
				for (int i = 1; i <= columnCount; i++) {
					String columnName = columns.getMetaData().getColumnName(i);
					Object object = columns.getObject(i);
					if (!"COLUMN_NAME".equals(columnName)) {
						if (object == null) {
							result.put(columnName, "");
						} else {
							result.put(columnName, object.toString());
						}
					}

				}
				map.put(column, result);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return map;
	}

	@Override
	public void commit() throws Exception {

	}

	@Override
	public void rollbcak() {

	}

	@Override
	public void closeSource() {
		this.hbase.close();
	}

	@Override
	public boolean put(JSONArray datas, String tableName) throws Exception {

		if (datas == null || datas.size() == 0 || tableName == null) {
			LOG.warn("Put wrong datas.......");
			return false;
		} else {
			return this.hbase.insertList(tableName, this.family, datas);
		}
	}

	@Override
	public boolean put(JSONObject data, String tableName) throws Exception {
		if (data == null || data.size() == 0 || tableName == null) {
			return false;
		} else {
			return this.hbase.insert(tableName, this.family, data);
		}
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			String tableName) throws Exception {

		if (data != null) {
			JSONArray jsonArray = selectFromPhoneix(getKeyArray(), condition,
					tableName, null);
			return updateWithRowKeys(data, jsonArray, tableName);
		}
		return false;
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			JSONArray datas, String tableName) throws Exception {
		if (data != null) {
			JSONArray jsonArray = selectFromPhoneix(getKeyArray(), condition,
					tableName, datas);
			return updateWithRowKeys(data, jsonArray, tableName);
		}
		return false;
	}

	@Override
	public boolean updateWithRowKeys(JSONObject data, JSONArray _keys,
			String tableName) throws Exception {
		return this.hbase.insert(tableName, _keys, this.family, data);
	}

	@Override
	public boolean updateWithRowKey(JSONObject data, String _key,
			String tableName) throws Exception {
		return this.hbase.insert(tableName, _key, this.family, data);
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, String tableName)
			throws Exception {
		return selectFromPhoneix(fields, condition, tableName, null);
	}

	@Override
	public JSONArray get(JSONArray fields, JSONArray _key, String tableName)
			throws Exception {
		JSONArray array = new JSONArray();
		int i = _key.size();
		for (int a = 0; a < i; a++) {
			String key = _key.getString(a);
			JSONObject b = getByKey(fields, key, tableName);
			array.add(b);
		}
		return array;
	}

	@Override
	public JSONObject getByKey(JSONArray fields, String _key, String tableName)
			throws Exception {
		return (JSONObject) this.hbase.get(tableName, _key, "cinfo", fields);

	}

	@Override
	public JSONArray get(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		return selectFromPhoneix(fields, condition, tableName, datas);
	}

	@Override
	public boolean deleteKeys(JSONArray fields, JSONArray _keys,
			String tableName) throws Exception {

		return this.hbase.deletes(tableName, this.family, _keys, fields);
	}

	@Override
	public boolean deleteKey(JSONArray fields, String key, String tableName)
			throws Exception {
		return this.hbase.deleteList(tableName, this.family, key, fields);
	}

	@Override
	public boolean delete(JSONArray fields, String condition, String tableName)
			throws Exception {
		JSONArray jsonArray = selectFromPhoneix(getKeyArray(), condition,
				tableName, null);
		return deleteKeys(fields, jsonArray, tableName);
	}

	@Override
	public boolean delete(JSONArray fields, JSONArray _keys, String tableName)
			throws Exception {
		return deleteKeys(fields, fields, tableName);
	}

	@Override
	public boolean delete(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		JSONArray jsonArray = selectFromPhoneix(getKeyArray(), condition,
				tableName, datas);
		return deleteKeys(fields, jsonArray, tableName);
	}

	private String[] selectArray(String rowkey, String qualifer,
			String tableName) {
		Connection connection = getConnection();
		String condition = "WHERE \"region_name\" = " + toSingn(rowkey);
		String select = PhoenixSqlUtil.parseToQuerySQL(getJsonArray(qualifer),
				condition, tableName, null);
		PreparedStatement stm1 = null;
		try {
			stm1 = connection.prepareStatement(select);
			ResultSet resultSet = stm1.executeQuery();
			Array array = null;
			while (resultSet.next()) {
				array = resultSet.getArray(qualifer);
			}
			return (String[]) array.getArray();
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				connection.close();
				stm1.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return null;
	}

	private boolean insertArray(String rowkey, String tableName,
			Object[] insertResult) {
		String sql = "UPSERT INTO " + toSingns(tableName) + " VALUES(?,?)";
		Connection connection = getConnection();
		PreparedStatement preparedStatement;
		try {
			preparedStatement = connection.prepareStatement(sql);
			Array insertArray = connection.createArrayOf("VARCHAR",
					insertResult);
			preparedStatement.setString(1, rowkey);
			preparedStatement.setArray(2, insertArray);
			preparedStatement.execute();
			connection.commit();
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			try {
				connection.rollback();
			} catch (SQLException e1) {
				LOG.error(e.getMessage(), e);
			}
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return true;
	}

	private Object[] operatorArray(String[] result, JSONArray array, boolean add) {
		Object[] object;
		if (add) {
			JSONArray resultArray = JSONArray.fromObject(result);
			resultArray.addAll(array);
			object = resultArray.toArray();
		} else {
			JSONArray resultArray = JSONArray.fromObject(result);
			resultArray.removeAll(array);
			object = resultArray.toArray();
		}
		return object;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, JSONArray add,
			String tableName) {
		String lockPath = "/fuhao/" + qualifer;
		InterProcessMutex interProcessMutex = new InterProcessMutex(
				this.curatorFramework, lockPath);
		try {
			interProcessMutex.acquire();
			String s = this.hbase.byGet(tableName, rowkey, "cinfo", qualifer);
			JSONArray array = JSONArray.fromObject(s);
			array.addAll(add);
			this.hbase.insert(tableName, rowkey, "cinfo", qualifer,
					array.toString());
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		try {
			interProcessMutex.release();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

		return true;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, String add,
			String tableName) throws Exception {
		return CAPadd(rowkey, qualifer, getJsonArray(add), tableName);
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, JSONArray remove,
			String tableName) {
		String lockPath = "/fuhao/" + qualifer;
		InterProcessMutex interProcessMutex = new InterProcessMutex(
				this.curatorFramework, lockPath);
		try {
			interProcessMutex.acquire();
			LOG.info(rowkey + " Locak has acquire......");
			String s = this.hbase.byGet(tableName, rowkey, "cinfo", qualifer);
			JSONArray array = JSONArray.fromObject(s);
			array.removeAll(remove);
			this.hbase.insert(tableName, rowkey, "cinfo", qualifer,
					array.toString());
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		try {
			interProcessMutex.release();
			LOG.info(rowkey + " Locak has release......");
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

		return true;

	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, String remove,
			String tableName) {

		return CAPremove(rowkey, qualifer, getJsonArray(remove), tableName);
	}

	@Override
	public void close() throws IOException {
		this.hbase.close();
	}

	private boolean putIntoPhoneix(JSONObject jsonObject, String tablenName) {

		return putIntoPhoneix(getJsonArray(jsonObject), tablenName);
	}

	private boolean putIntoPhoneix(JSONArray jsonArray, String tablenName) {
		Connection connection = null;
		PreparedStatement pst = null;
		try {
			connection = DriverManager.getConnection(this.url);
			int size = jsonArray.size();
			String insertSql = PhoenixSqlUtil.parseToInsertSQL(
					jsonArray.getJSONObject(0), tableName, null);
			pst = connection.prepareStatement(insertSql);
			for (int i = 0; i < size; i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(0);
				Collection values = jsonObject.values();
				Iterator vals = values.iterator();
				int index = 1;
				while (vals.hasNext()) {
					pst.setObject(index++, vals.next());
				}
				pst.addBatch();
			}
			pst.executeBatch();
			connection.commit();
			return true;
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			try {
				connection.rollback();
			} catch (SQLException e1) {
				LOG.error(e.getMessage(), e);
			}
		} finally {
			try {
				connection.close();
				pst.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return false;
	}

	private JSONArray selectFromPhoneix(JSONArray fields, String condition,
			String tableName, JSONArray datas) {
		String selectSql = PhoenixSqlUtil.parseToQuerySQL(fields, condition,
				tableName, null);
		LOG.info("!!" + selectSql);
		Connection connection = null;
		JSONArray jsonArray = new JSONArray();
		try {
			connection = DriverManager.getConnection(this.url);
			PreparedStatement pst = connection.prepareStatement(selectSql);
			if (datas != null) {
				int i = datas.size();
				for (int l = 0; l < i; l++) {
					pst.setObject(l, datas.get(l));
				}
			}
			ResultSet res = pst.executeQuery();
			while (res.next()) {
				JSONObject obj = new JSONObject();
				for (int i = 0; i < fields.size(); i++) {
					String field = fields.getString(i);
					Object o = res.getObject(field);
					obj.put(field, o);
				}
				jsonArray.add(obj);
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		LOG.info("!!" + jsonArray.toString());
		return jsonArray;
	}

	private JSONArray getKeyArray() {
		JSONArray j = new JSONArray();
		j.add(KEY);
		return j;
	}

	private static JSONArray getJsonArray(Object... json) {
		JSONArray jsonArray = new JSONArray();
		for (Object j : json) {
			jsonArray.add(j);
		}
		return jsonArray;
	}

	private Connection getConnection() {
		try {
			Connection connection = DriverManager.getConnection(this.url);
			System.out.println("connection" + connection);
			return connection;
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}

	private String toSingn(String string) {
		return "\'" + string + "\'";
	}

	private String toSingns(String string) {
		return "\"" + string + "\"";
	}

}
