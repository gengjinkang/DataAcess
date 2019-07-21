package com.fuhao.data.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.fuhao.data.column.RedisColumnManager;

public class MySqlmetadataConnection {
	private Connection conn;
	
	private Statement stat;
	
	private PreparedStatement pstat;
	
	private Map<String ,Map<String ,String>> map = new HashMap<String, Map<String,String>>();
	
	private Map<String ,String> Messagemap = null;

	String sourcename = "MySql";
	String url = "jdbc:mysql://192.168.1.27:3306/xxd";
	String user = "root";
	String pwd = "fuhao";
	String schema = null;
	
	public MySqlmetadataConnection() throws ClassNotFoundException, SQLException {
		init();
	}
	@Test
	public void init() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(url, user, pwd);
	}
	
	public Map<String ,Map<String, String>> getMetaData() throws SQLException {
		DatabaseMetaData dbm = conn.getMetaData();
		String[] types = {"TABLE"};
		ResultSet set = dbm.getCatalogs();
		while(set.next()) {
			schema = set.getString("TABLE_CAT");
			if(!schema.equals("sys")) {
				ResultSet tableResult = dbm.getTables(schema, "%", "%", types);
				while(tableResult.next()) {
					String tablename = tableResult.getString("TABLE_NAME");						
					ResultSet colSet = dbm.getColumns(null, "%", tablename, "%");
					while(colSet.next()) {
						String columnName = colSet.getString("COLUMN_NAME");
						String typeName = colSet.getString("TYPE_NAME");	
						//System.out.println(sourcename+"--"+schema+"--"+tablename+"--"+columnName+"--"+typeName);
						Messagemap=new HashMap<String, String>();
						Messagemap.put("TYPE", typeName);
						Messagemap.put("TABLENAME", tablename);
						Messagemap.put("SOURCENAME", sourcename);
						Messagemap.put("SCHEMA", schema);
						map.put(columnName, Messagemap);
					}
				}
			}
			
		}
		return map;
		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		MySqlmetadataConnection m =new MySqlmetadataConnection();
		Map<String, Map<String, String>> metaData = m.getMetaData();
		RedisColumnManager manager=new RedisColumnManager();
		manager.saveAllInfo(metaData);
		
	}
	private static void printHash(Map map){
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
}
