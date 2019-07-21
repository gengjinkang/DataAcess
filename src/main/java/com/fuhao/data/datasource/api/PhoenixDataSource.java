package com.fuhao.data.datasource.api;

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
import java.util.Properties;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.fuhao.data.column.ColumnManager;
import com.fuhao.data.config.Configurations;
import com.fuhao.data.datasource.DataSource;
import com.fuhao.data.util.PhoenixSqlUtil;

public class PhoenixDataSource implements DataSource {

	private static final Logger LOG = Logger.getLogger(SqlDataSource.class);
	private static final String SOURCENAME="phoenix";
	private ThreadLocal<Boolean> trascationFlag;
	private ThreadLocal<Connection> connection;
	private String driver;
	private String url;
	private String ZOOKEEPER_ADDR;
	
	private String KEY = "_key";
	private String tableName = "";
	private int SESSION_OUTIME = 5000;
	private ColumnManager columnManager;

	public PhoenixDataSource() {
		init();
	}

	/**
	 * 初始化数据源
	 * 
	 * @throws SQLException
	 */
	private void init() {
		try {
			Properties config = Configurations.PROPERTIES;
			this.driver = config.getProperty("phoenix.driver");
			this.url = config.getProperty("phoenix.url");
			this.ZOOKEEPER_ADDR = config.getProperty("zookeeper.hosts");
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		trascationFlag = new ThreadLocal<Boolean>();
		trascationFlag.set(false);// 默认情况下不开启事务
		connection = new ThreadLocal<Connection>();
		connection.set(null);
	}

	@Override
	public boolean put(JSONArray jsonArray, String tableName)
			throws SQLException {
		boolean res = true;
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject json = jsonArray.getJSONObject(i);
			put(json, tableName);
		}
		return res;
	}

	/**
	 * 将json中的数据存入到指定的表中
	 *
	 * @param json
	 *            需要插入的数据
	 * @param tableName
	 *            表名
	 * @throws SQLException
	 */
	public boolean put(JSONObject json, String tableName) throws SQLException {
		JSONObject jsonObject = new JSONObject();
		Iterator iterator = json.keys();
		while (iterator.hasNext()) {
			String cloumn = (String) iterator.next();
			String type = columnManager.getType(cloumn);
			jsonObject.put(cloumn, type);
		}
		String insertSQL = PhoenixSqlUtil.parseToInsertSQL(json, tableName,
				jsonObject);
		return executeSQL(insertSQL, json);
	}

	/**
	 * 根据所给的sql语句，以及需要操作的数据，返回sql语句的执行结果
	 *
	 * @param sql
	 *            执行的sql语句
	 * @param values
	 *            需要绑定到占位符上(即:？)的数据
	 * @return boolean 返回执行结果，执行成功返回true，失败则返回false
	 * @throws SQLException
	 */
	public boolean executeSQL(String sql, JSONObject values)
			throws SQLException {
		Connection conn = getConnection();
		PreparedStatement pst = null;
		pst = conn.prepareStatement(sql);
		bindValue(values, pst);
		pst.execute();
		return true;
	}

	/**
	 * 根据sql语句查找对应的字段，并将值返回
	 *
	 * @param fields
	 *            要查询的字段
	 * @param querySQL
	 *            要执行的sql语句
	 * @return 将查到的记录，以{@link JSONArray}对象返回
	 * @throws SQLException
	 */
	private JSONArray executeQuery(JSONArray fields, String querySQL)
			throws SQLException {
		JSONArray resArray = new JSONArray();
		Connection conn = getConnection();
		PreparedStatement pst = conn.prepareStatement(querySQL);
		ResultSet res = pst.executeQuery();
		while (res.next()) {
			JSONObject obj = new JSONObject();
			for (int i = 0; i < fields.size(); i++) {
				String field = fields.getString(i);
				obj.put(field, res.getObject(field));
			}
			resArray.add(obj);
		}
		return resArray;
	}

	/**
	 * 返回一个连接对象。根据事务是否开启，如果开启事务，则从ThreadLocal中去获取连接对象，如果没有开启事务，则直接从连接池中获取连接对象。
	 *
	 * @return {@link Connection}返回一个连接对象
	 * @throws SQLException
	 */
	private Connection getConnection() {
		Connection conn = connection.get();
		// 判断当前线程变量是否包含Connection对象
		try {
			if (conn == null || conn.isClosed()) {
				conn = DriverManager.getConnection(this.url);
				connection.set(conn);
			}
			// 判断是否开启了事务
			boolean tr = trascationFlag.get();
			conn.setAutoCommit(!tr);// 当开启事务的时候关闭自动提交
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.debug(conn);
		return conn;
	}

	/**
	 * 将json中字段的值映射到{@link PreparedStatement}对象所要执行的sql语句的占位符上
	 *
	 * @param json
	 *            {@link JSONObject}要进行操作的数据
	 * @param pst
	 *            {@link PreparedStatement}目标 对象
	 * @throws SQLException
	 */
	private void bindValue(JSONObject json, PreparedStatement pst)
			throws SQLException {
		Collection values = json.values();
		Iterator vals = values.iterator();
		int index = 1;
		while (vals.hasNext()) {
			pst.setObject(index++, vals.next());
		}
	}

	@Override
	public String getSourceName() {
		return SOURCENAME;
	}

	@Override
	public Map<String, Map<String, String>> getInfo() {
	
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(this.url);
			LOG.debug(connection);
			DatabaseMetaData metaData = connection.getMetaData();

			ResultSet columns = metaData.getColumns(null, null, this.tableName,
					"%");
			int columnCount = columns.getMetaData().getColumnCount();// 获取列的数量
			Map<String, Map<String, String>> map = new HashMap<>();
			while (columns.next()) {
				String column = columns.getString("COLUMN_NAME");
				Map<String, String> result = new HashMap<>(columnCount);
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
			return map;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void close() {
		try {
			Connection conn = this.connection.get();
			if (!conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void beginTransaction() {
		trascationFlag.set(true);
	}

	@Override
	public void commit() {
		try {
			if (trascationFlag.get()) {
				connection.get().commit();
				trascationFlag.set(false);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rollbcak() {
		try {
			connection.get().rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeSource() {

	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			String tableName) throws SQLException {
		JSONArray query = new JSONArray();
		query.add(KEY);
		JSONArray keys = get(query, condition, tableName);
		return updateWithRowKeys(data, keys, tableName);
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			JSONArray datas, String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKeys(JSONObject data, JSONArray _keys,
			String tableName) throws SQLException {
		JSONArray insert = new JSONArray();
		for (int i = 0; i < _keys.size(); i++) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.putAll(data);
			jsonObject.putAll(_keys.getJSONObject(i));
			insert.add(jsonObject);
		}
		return put(insert, tableName);
	}

	@Override
	public boolean updateWithRowKey(JSONObject data, String _key,
			String tableName) throws SQLException {
		JSONArray array = new JSONArray();
		array.add(_key);
		return updateWithRowKeys(data, array, tableName);
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, String tableName)
			throws SQLException {
		int length = fields.size();
		JSONObject jsonObject = new JSONObject();
		for (int i = 0; i < length; i++) {
			String cloumn = fields.getString(i);
			String type = columnManager.getType(cloumn);
			jsonObject.put(cloumn, type);
		}
		String querySQL = PhoenixSqlUtil.parseToQuerySQL(fields, condition,
				tableName, jsonObject);
		LOG.debug(querySQL);
		JSONArray resArray = executeQuery(fields, querySQL);
		return resArray;
	}

	@Override
	public JSONArray get(JSONArray fields, JSONArray _key, String tableName)
			throws Exception {
		JSONArray array = new JSONArray();
		int l = _key.size();
		for (int i = 0; i < l; i++) {
			String key = _key.getString(i);
			array.add(getByKey(fields, key, tableName));
		}
		return array;
	}

	@Override
	public JSONObject getByKey(JSONArray fields, String _key, String tableName)
			throws Exception {
		String condition = "where \"" + KEY + "\"=\'" + _key + "\'";
		return (JSONObject) get(fields, condition, tableName).get(0);
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		return null;
	}

	@Override
	public boolean deleteKeys(JSONArray fields, JSONArray _keys,
			String tableName) throws Exception {
		int l = _keys.size();
		for (int i = 0; i < l; i++) {
			String key = _keys.getString(i);
			deleteKey(fields, key, tableName);
		}
		return true;
	}

	public boolean deleteKey(JSONArray fields, String key, String tableName)
			throws Exception {
		String condition = "where \"" + KEY + "\"=\'" + key + "\'";
		return delete(fields, condition, tableName);
	}

	@Override
	public boolean delete(JSONArray fields, String condition, String tableName)
			throws SQLException {
		String deleteSQL = PhoenixSqlUtil
				.parseToDeleteSQL(condition, tableName);
		LOG.debug(deleteSQL);
		Connection conn = getConnection();
		PreparedStatement pst = conn.prepareStatement(deleteSQL);
		pst.execute();
		return true;
	}

	@Override
	public boolean delete(JSONArray fields, JSONArray _keys, String tableName) {
		return false;
	}

	@Override
	public boolean delete(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, JSONArray add,
			String tableName) {

		return capOperation(rowkey, qualifer, add, tableName, true);
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, String add,
			String tableName) {
		JSONArray adds = new JSONArray();
		adds.add(add);
		return capOperation(rowkey, qualifer, adds, tableName, true);
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, JSONArray remove,
			String tableName) {
		// TODO Auto-generated method stub
		return capOperation(rowkey, qualifer, remove, tableName, false);
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, String remove,
			String tableName) {
		JSONArray removeArray = new JSONArray();
		removeArray.add(remove);
		return capOperation(rowkey, qualifer, removeArray, tableName, false);

	}

	public boolean capOperation(String rowkey, String qualifer, JSONArray add,
			String tableName, boolean way) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url);
			connection.setAutoCommit(false);
			JSONArray jsonArray = new JSONArray();
			jsonArray.add(qualifer);
			String condition = "where \"" + KEY + "\"=\'" + rowkey + "\'";
			String select = PhoenixSqlUtil.parseToQuerySQL(jsonArray,
					condition, tableName, null);

			JSONArray resultArray = JSONArray.fromObject(select(select,
					connection));
			if (way) {
				resultArray.addAll(add);
			} else {
				resultArray.removeAll(add);
			}
			String insertArray = resultArray.toString();
			JSONObject put = new JSONObject();
			put.put(KEY, rowkey);
			put.put(qualifer, insertArray);
			String insert = PhoenixSqlUtil.parseToInsertSQL(put, tableName,
					null);
			insert(insert, connection, rowkey, insertArray);
			connection.commit();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		return true;
	}

	private String select(String sql, Connection connection)
			throws SQLException {
		PreparedStatement ps;
		ps = connection.prepareStatement(sql);
		ResultSet set = ps.executeQuery();
		String result = "";
		while (set.next()) {
			result = set.getString(1);
		}
		return result;
	}

	private boolean insert(String sql, Connection connection, String rowkey,
			String insertArray) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, rowkey);
		preparedStatement.setString(2, insertArray);
		preparedStatement.executeUpdate();
		preparedStatement.close();
		return true;
	}

}
