package com.fuhao.data.datasource.api;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.fuhao.data.config.Configurations;
import com.fuhao.data.datasource.DataSource;
import com.fuhao.data.util.ParseSQLUtil;

public class PostgreSqlDataSource implements DataSource {
	private static final Logger LOG = Logger.getLogger(PostgreSqlDataSource.class);
	private static final String SOURCENAME="postgresql";
	private DruidDataSource dataSource;
	private ThreadLocal<Boolean> trascationFlag;
	private ThreadLocal<Connection> connection;
	private String schema = null;
	private Map<String ,Map<String ,String>> map = new HashMap<String, Map<String,String>>();
	private Map<String ,String> Messagemap = null;

	public PostgreSqlDataSource() {
		this(new DruidDataSource());
	}

	public PostgreSqlDataSource(DruidDataSource dataSource) {
		this.dataSource = dataSource;
		init();
	}

	/**
	 * 初始化数据源
	 * 
	 * @throws SQLException
	 */
	private void init()  {
		dataSource.setDriverClassName("org.postgresql.Driver");
		dataSource.setUrl(Configurations.PROPERTIES.getProperty("postgresql.url"));
		dataSource.setUsername(Configurations.PROPERTIES
				.getProperty("postgresql.username"));
		dataSource.setPassword(Configurations.PROPERTIES
				.getProperty("postgresql.userpw"));
		trascationFlag = new ThreadLocal<Boolean>();
		trascationFlag.set(false);// 默认情况下不开启事务
		connection = new ThreadLocal<Connection>();
		connection.set(null);

	}

	public DruidDataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DruidDataSource dataSource) throws SQLException {
		this.dataSource = dataSource;
		init();
	}

	@Override
	public boolean put(JSONArray jsonArray, String tableName)
			throws SQLException {
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject json = jsonArray.getJSONObject(i);
			put(json, tableName);
		}
		return true;

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
		String insertSQL = ParseSQLUtil.parseToInsertSQL(json, tableName);
		LOG.debug(insertSQL);

		return executeSQL(insertSQL, json);
	}

	/**
	 * 根据所给的sql语句，以及需要操作的数据，返回sql语句的执行结果
	 * 
	 * @param sql
	 *            执行的sql语句
	 * @param values
	 *            需要绑定到占位符上(即:？)的数据
	 * @return boolean 返回执行结果，执行成功返回true，失败则抛出异常
	 * @throws SQLException
	 */
	public boolean executeSQL(String sql, JSONObject values)
			throws SQLException {
		Connection conn = getConnection();
		PreparedStatement pst = conn.prepareStatement(sql);
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
				obj.put(field, res.getObject(field).toString());
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
	private Connection getConnection() throws SQLException {
		Connection conn = connection.get();
		// 判断当前线程变量是否包含Connection对象
		if (conn == null || conn.isClosed()) {
			conn = dataSource.getConnection();
			connection.set(conn);
		}
		// 判断是否开启了事务
		boolean tr = trascationFlag.get();
		conn.setAutoCommit(!tr);// 当开启事务的时候关闭自动提交
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
		DatabaseMetaData dbm;
		try {
			Connection conn = getConnection();
			dbm = conn.getMetaData();
			String[] types = {"TABLE"};
			ResultSet set = dbm.getCatalogs();
			while(set.next()) {
				schema = set.getString("TABLE_CAT");
					ResultSet tableResult = dbm.getTables(schema, "%", "%", types);
					while(tableResult.next()) {
						String tablename = tableResult.getString("TABLE_NAME");						
						ResultSet colSet = dbm.getColumns(null, "%", tablename, "%");
						while(colSet.next()) {
							String columnName = colSet.getString("COLUMN_NAME");
							String typeName = colSet.getString("TYPE_NAME");	
							System.out.println(SOURCENAME+"--"+schema+"--"+tablename+"--"+columnName+"--"+typeName);
							Messagemap=new HashMap<String, String>();
							Messagemap.put("TYPE", typeName);
							Messagemap.put("TABLENAME", tablename);
							Messagemap.put("SOURCENAME", SOURCENAME);
							Messagemap.put("SCHEMA", schema);
							map.put(columnName, Messagemap);
					}
				}
				
			}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}
	

	public void close() {
		try {
			Connection conn= this.connection.get();
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
	public void commit() throws SQLException {
			if (trascationFlag.get()) {
				connection.get().commit();
				trascationFlag.set(false);
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
		if (!this.dataSource.isClosed()) {
			this.dataSource.close();
		}
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			String tableName) throws SQLException {
		String updataSQL = ParseSQLUtil.parseToUpdateSQL(data, condition,
				tableName);
		return executeSQL(updataSQL, data);
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition, JSONArray datas, String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKeys(JSONObject data, JSONArray _keys,
			String tableName) {
		return false;
	}

	@Override
	public boolean updateWithRowKey(JSONObject data, String _key,
			String tableName) {
		return false;
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, String tableName)
			throws SQLException {
		String querySQL = ParseSQLUtil.parseToQuerySQL(fields, condition,
				tableName);
		JSONArray resArray = executeQuery(fields, querySQL);
		return resArray;
	}

	@Override
	public JSONArray get(JSONArray fields, JSONArray _key, String tableName) {
		return null;
	}

	@Override
	public JSONObject getByKey(JSONArray fields, String _key, String tableName) {
		return null;
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, JSONArray datas, String tableName) throws Exception {
		return null;
	}


	@Override
	public boolean deleteKeys(JSONArray fields, JSONArray _keys, String tableName) {
		return false;
	}

	@Override
	public boolean deleteKey(JSONArray fields, String key, String tableName) throws Exception {
		return false;
	}


	@Override
	public boolean delete(JSONArray fields, String condition, String tableName)
			throws SQLException {
		String deleteSQL = ParseSQLUtil.parseToDeleteSQL(condition, tableName);
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
	public boolean delete(JSONArray fields, String condition, JSONArray datas, String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, JSONArray add,
			String tableName) {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, String add,
			String tableName) {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, JSONArray remove,
			String tableName) {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, String remove,
			String tableName) {
		return false;
	}

}
