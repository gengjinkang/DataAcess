package com.fuhao.data.access;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.fuhao.data.column.ColumnManager;
import com.fuhao.data.column.RedisColumnManager;
import com.fuhao.data.datasource.DataSource;
import com.fuhao.data.datasource.api.HbaseDataSource;
import com.fuhao.data.datasource.api.SqlDataSource;
import com.fuhao.data.util.DataFormat;
import com.fuhao.data.util.SnowflakeIdWorker;
import com.google.gson.JsonObject;

public class FuHaoAccess implements FuHaoData {
	private static final Logger logger = Logger.getLogger(FuHaoAccess.class);
	private final SnowflakeIdWorker worker;
	private static final String KEY = "_key";
	private ColumnManager manager;
	/* 获取数据源 */
	private Map<String, DataSource> dataSourceMap;
	private static FuHaoAccess acess = null;

	public static synchronized FuHaoData getInstance() {
		if (acess == null) {
			acess = new FuHaoAccess();
		}
		return acess;
	}
	private FuHaoAccess() {
		logger.info("----- FuHaoAccess init ------");
		this.worker = new SnowflakeIdWorker();
		this.dataSourceMap = new HashMap<>();
		this.manager = new RedisColumnManager();
		managerInit();
		logger.info("----- FuHaoAccess init finshed ------");
	}
	
	@Override
	public void beginTransaction() {
		
	}

	@Override
	public boolean put(JSONArray datas) throws Exception {
		JSONObject jsonObject = split(datas);
		System.out.println(jsonObject);
		String source = jsonObject.getString("sourceName");
		String tableName = jsonObject.getString("tableName");
		JSONArray formatdata = jsonObject.getJSONArray("data");
		DataSource dataSource = this.dataSourceMap.get(source);
		return false;
	}

	@Override
	public boolean put(JSONArray datas, String tableName) throws Exception {

		return true;
	}

	@Override
	public boolean put(JSONObject data) throws Exception {
		JSONObject split = split2(data);
		Iterator keys = split.keys();
		while(keys.hasNext()) {
			String sourceName = (String) keys.next();
			if(sourceName.equals("Mysql")) {
				 JSONObject jtablename = (JSONObject) split.get("Mysql");
				 Iterator keys2 = jtablename.keys();
				 
			}
		}
		System.out.println(split);
		return false;
	}

	@Override
	public boolean put(JSONObject data, String tableName) throws Exception {
		return false;
	}

	@Override
	public JSONArray putReturnKeys(JSONArray datas) throws Exception {
		return null;
	}

	@Override
	public JSONArray putReturnKeys(JSONArray datas, String tableName)
			throws Exception {
		return null;
	}

	@Override
	public String putReturnKey(JSONObject data) throws Exception {
		return null;
	}

	@Override
	public String putReturnKey(JSONObject data, String tableName)
			throws Exception {
		return null;
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition)
			throws Exception {
		return false;
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean updateWithCondition(JSONObject data, String condition,
			JSONArray datas, String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKeys(JSONObject data, JSONArray _keys)
			throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKeys(JSONObject data, JSONArray _keys,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKey(JSONObject data, String _key)
			throws Exception {
		return false;
	}

	@Override
	public boolean updateWithRowKey(JSONObject data, String _key,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public JSONArray get(JSONArray fields, String condition) throws Exception {
		JSONObject jsonObject = splitGet(fields);
		String source = jsonObject.getString("sourceName");
		String tableName = jsonObject.getString("tableName");
		DataSource dataSource = this.dataSourceMap.get(source);
		return dataSource.get(fields, condition, tableName);
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, String tableName)
			throws Exception {
		return null;
	}

	@Override
	public JSONArray get(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		return null;
	}

	@Override
	public JSONArray get(JSONArray fields, JSONArray _key) throws Exception {
		return null;
	}

	@Override
	public JSONArray get(JSONArray fields, JSONArray _key, String tableName)
			throws Exception {
		return null;
	}

	@Override
	public JSONObject getByKey(JSONArray fields, String _key) throws Exception {
		return null;
	}

	@Override
	public JSONObject getByKey(JSONArray fields, String _key, String tableName)
			throws Exception {
		return null;
	}

	@Override
	public boolean delete(JSONArray fields, JSONArray _keys) throws Exception {
		return false;
	}

	@Override
	public boolean delete(JSONArray fields, JSONArray _keys, String tableName)
			throws Exception {
		return false;
	}

	@Override
	public boolean delete(JSONArray fields, String condition) throws Exception {
		JSONObject jsonObject = splitGet(fields);
		String source = jsonObject.getString("sourceName");
		String tableName = jsonObject.getString("tableName");
		DataSource dataSource = this.dataSourceMap.get(source);
		return dataSource.delete(fields, condition, tableName);
	}

	@Override
	public boolean delete(JSONArray fields, String condition, String tableName)
			throws Exception {
		return false;
	}

	@Override
	public boolean delete(JSONArray fields, String condition, JSONArray datas,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, JSONArray add)
			throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, JSONArray add,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, String add)
			throws Exception {
		return false;
	}

	@Override
	public boolean CAPadd(String rowkey, String qualifer, String add,
			String tableName) throws Exception {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, JSONArray remove) {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, JSONArray remove,
			String tableName) {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, String remove) {
		return false;
	}

	@Override
	public boolean CAPremove(String rowkey, String qualifer, String remove,
			String tableName) {
		return false;
	}

	@Override
	public void commit() throws Exception {

	}

	@Override
	public void close() throws IOException {

	}

	private void managerInit() {
		DataSource HbaseSource = new HbaseDataSource();
		this.dataSourceMap.put("Hbase", HbaseSource);
//		this.manager.saveAllInfo(HbaseSource.getInfo());
		DataSource SqlSource = new SqlDataSource();
		this.dataSourceMap.put("MySql", SqlSource);


	}

	private JSONObject split(JSONArray datas) {
		JSONObject jsonObject = new JSONObject();
		boolean put = false;
		for (int i = 0; i < datas.size(); i++) {
			JSONObject data = datas.getJSONObject(i);
			Iterator iterator = data.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Map.Entry) iterator.next();
				String cloumn = (String) entry.getKey();
				String value = entry.getValue().toString();
				String sourceName = this.manager.getSourceName(cloumn);
				String tableName = this.manager.getTableName(cloumn);
				tableName = tableName != null ? tableName : this.manager.get(
						cloumn, "TABLE_NAME");
				String type = this.manager.getType(cloumn);
				if (!put) {
					jsonObject.put("tableName", tableName);
					jsonObject.put("sourceName", sourceName);
					put = true;
				}
				Object o = DataFormat.format(type, value);
				data.put(cloumn, o);
			}
		}
		jsonObject.put("data", datas);
		return jsonObject;
	}

	
	
	/**
	 * 将数据按照数据源和表进行拆分
	 * @param data
	 * @return
	 */
	private JSONObject split2(JSONObject data) {
		JSONObject datas=new JSONObject();
		if(!data.containsKey("_key"))return null;
		Object key=data.get("_key");
		Set<String> keySet = data.keySet();
		for (String column : keySet) {
			if(!"_key".equals(column)){
				String tName=manager.getTableName(column.toUpperCase());
				tName=tName!=null?tName:manager.get(column.toUpperCase(),"TABLE_NAME");
				String sName=manager.getSourceName(column.toUpperCase());
				if(sName==null||tName==null)continue;
				JSONObject tbs=(JSONObject) datas.get(sName);
				if(tbs==null){
					tbs=new JSONObject();
				}
				JSONObject tb = (JSONObject) tbs.get(tName);
				if(tb==null){
					tb=new JSONObject();
					tb.put("_key", key);
					System.out.println(tb);
				}
				tb.put(column, data.get(column));
				tbs.put(tName, tb);
				datas.put(sName, tbs);
			}
		}
		return datas;
	}
	
	public JSONObject split3() {
		JSONObject jsonObject = new JSONObject();
		JSONArray j = new JSONArray();
		
		return null;
	}
		
	private JSONObject splitGet(JSONArray fields) {
		JSONObject jsonObject = new JSONObject();
		JSONArray j = new JSONArray();
		boolean put = false;
		for (int i = 0; i < fields.size(); i++) {
//			j = new JSONArray();
//			if (!put) {
//				jsonObject.put("tableName", tableName);
//				jsonObject.put("sourceName", sourceName);
//				put = true;
//			}
//			j.add(cloumn);
		}
		jsonObject.put("field", j);
		return jsonObject;
	}
	
}
