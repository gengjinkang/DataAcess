package com.fuhao.test;

import java.sql.SQLException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fuhao.data.column.RedisColumnManager;
import com.fuhao.data.datasource.DataSource;
import com.fuhao.data.datasource.api.PhoenixDataSource;
import com.fuhao.data.datasource.api.SqlDataSource;

public class MySqlmetadataTest {
	DataSource dataSource = null;
	
	@Before
	public void setUp() throws SQLException {
		dataSource = new SqlDataSource();
	}
	
	@Test
	public void connect() throws Exception {
	
		RedisColumnManager r = new RedisColumnManager();
		PhoenixDataSource dataSource=new PhoenixDataSource();
		Map<String, Map<String, String>> info = dataSource.getInfo();
		System.out.println(info);
		r.saveAllInfo(info);
		String tableName = r.getTableName("passwd".toUpperCase());
		System.out.println(r.getInfo("passwd".toUpperCase()));
	}
	
}
