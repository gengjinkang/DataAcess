package com.fuhao.data.column;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 */
public class RedisColumnManager implements ColumnManager {

	private JedisCluster jedis;

	public RedisColumnManager() {
		Set<HostAndPort> hpSet = new HashSet<HostAndPort>();
		HostAndPort hp1 = new HostAndPort("hadoop", 5270);
		HostAndPort hp2 = new HostAndPort("slave2", 5270);
		HostAndPort hp3 = new HostAndPort("slave3", 5270);
		HostAndPort hp4 = new HostAndPort("hadoop", 5271);
		HostAndPort hp5 = new HostAndPort("slave2", 5271);
		HostAndPort hp6 = new HostAndPort("slave3", 5271);
		hpSet.add(hp1);
		hpSet.add(hp2);
		hpSet.add(hp3);
		hpSet.add(hp4);
		hpSet.add(hp5);
		hpSet.add(hp6);
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(60000);
		config.setMaxIdle(1000);
		config.setMaxWaitMillis(3000);
		config.setTestOnBorrow(true);
		jedis = new JedisCluster(hpSet, config);
	}

	@Override
	public Map<String, String> getInfo(String column) {
		if (column == null || "".equals(column)) {
			return null;
		}
		Map<String, String> info = jedis.hgetAll(column);

		return info;
	}

	@Override
	public String getType(String column) {
		return jedis.hget(column, "TYPE");
	}

	@Override
	public String getTableName(String column) {
		return jedis.hget(column, "TABLENAME");
	}

	@Override
	public String getSourceName(String column) {
		return jedis.hget(column, "SOURCENAME");
	}

	@Override
	public String getSchema(String column) {
		return jedis.hget(column, "SCHEMA");
	}

	@Override
	public String get(String column, String info) {
		return jedis.hget(column, info);
	}

	@Override
	public void saveAllInfo(Map<String, Map<String, String>> columnInfo) {
		System.out.println(columnInfo);
		Set<Entry<String, Map<String, String>>> entrySet = columnInfo.entrySet();
		for (Entry<String, Map<String, String>> entry : entrySet) {
			String columnName = entry.getKey();
			Map<String, String> columnMeta = entry.getValue();
			jedis.hmset(columnName, columnMeta);
		}
	}
}
