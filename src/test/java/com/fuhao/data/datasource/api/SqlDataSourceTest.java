package com.fuhao.data.datasource.api;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Before;
import org.junit.Test;

import com.fuhao.data.datasource.DataSource;
import com.mysql.cj.jdbc.MysqlDataSource;

public class SqlDataSourceTest {
	PostgreSqlDataSource dataSource = null;
	
	@Before
	public void setUp() throws SQLException {
		dataSource = new PostgreSqlDataSource();
	}
	@Test
	public void testMetaInfo(){
		Map<String, Map<String, String>> info = dataSource.getInfo();
//		printHash(info);
	}
	private void printHash(Map map){
		Set<Entry<String, Map<String, String>>> entrySet = map.entrySet();
		for (Entry<String, Map<String, String>> entry : entrySet) {
			String key=entry.getKey();
			System.out.println(key+":");
			Map<String, String> value = entry.getValue();
			Set<Entry<String, String>> entrySet2 = value.entrySet();
			for (Entry<String, String> entry2 : entrySet2) {
				String key2=entry2.getKey();
				String val2=entry2.getValue();
				System.out.println("\t"+key2+":"+val2);
			}
		}
	}
	@Test
	public void testdelete() throws Exception {
		JSONArray array=new JSONArray();
		boolean delete = dataSource.delete(array, "where \"id\"=3", "user_tb2");
		System.out.println(delete);
	}
	@Test
	public void testget() throws Exception {

		JSONArray array=new JSONArray();
		array.add("_key");
		JSONArray jsonArray = dataSource.get(array, "where mt1_name = '张三'", "mysql_t1");
		System.out.println(jsonArray);
	}
	@Test
	public void testUpdate() throws Exception {

		JSONObject obj = new JSONObject();
		obj.put("date","2018-08-31");
		dataSource.updateWithCondition(obj, "where name='maybe'", "user_tb1");
//		dataSource.commit();
	}
	/**
	 * 没有手动创建事务
	 * @throws Exception 
	 */
	@Test
	public void testPut() throws Exception {

		JSONObject obj = new JSONObject();
		obj.put("name", "张三");
		obj.put("date",new Date(System.currentTimeMillis()));
		//dataSource.beginTransaction();
		dataSource.put(obj, "user_tb1");
//		dataSource.commit();
	}

	@Test
	public void testManual() throws Exception {
		/**
		 * 手动创建事务
		 */
		dataSource.beginTransaction();
		JSONObject obj = new JSONObject();
		obj.put("id", 1002);
		obj.put("name", "demo");
		obj.put("age", 22);
		dataSource.put(obj, "emp");
		dataSource.close();
		JSONObject obj2 = new JSONObject();
		obj2.put("id", 1003);
		obj2.put("name", "demo");
		obj2.put("age", 22);
		dataSource.put(obj2, "emp");
		dataSource.commit();

	}
	@Test
	public void testRollback() throws Exception {
		/**
		 * 手动创建事务
		 */
		dataSource.beginTransaction();
		JSONObject obj = new JSONObject();
		obj.put("id", 1004);
		obj.put("name", "demo");
		obj.put("age", 22);
		dataSource.put(obj, "emp");
		dataSource.rollbcak();
		JSONObject obj2 = new JSONObject();
		obj2.put("id", 1005);
		obj2.put("name", "demo");
		obj2.put("age", 22);
		dataSource.put(obj2, "emp");
		dataSource.rollbcak();
		dataSource.commit();

	}
	@Test
	public void testPutJSONArray() throws Exception{
		JSONObject obj = new JSONObject();
		obj.put("id", 1004);
		obj.put("name", "demo");
		obj.put("age", 22);
		JSONObject obj2 = new JSONObject();
		obj2.put("id", 1004);
		obj2.put("name", "demo");
		obj2.put("age", 22);
		JSONArray objs=new JSONArray();
		objs.add(obj);
		objs.add(obj2);
		System.out.println(dataSource.put(objs, "emp"));
	}
	
}
