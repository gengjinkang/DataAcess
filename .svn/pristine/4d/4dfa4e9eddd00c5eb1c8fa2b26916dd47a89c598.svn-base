package com.fuhao.data.fhbase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.fuhao.data.config.Configurations;
import com.fuhao.data.util.JsonUtil;
import com.fuhao.data.util.PhType;
import com.fuhao.data.util.PhTypeUtil;
import com.fuhao.data.util.ThreadPoolUtil;

/**
 * Created by root on 7/10/17.
 */
public class HBaseClient implements FuhaoHbase {

	private ThreadPoolUtil threadPool;
	private static Logger logger = Logger.getLogger(HBaseClient.class);
	private String _key = "_key";
	private Configuration configuration;
	private Connection connection;
	private String port;
	private String address;
	private JSONArray longString = JsonUtil.builderArray("g0003", "g0008",
			"c0011");
	private JSONArray doubleString = JsonUtil.builderArray("g0004", "g0005",
			"g0006", "g0009");

	public HBaseClient(String port, String address) {
		this.port = port;
		this.address = address;
		init();
	}

	public HBaseClient() {
		this.port = Configurations.PROPERTIES
				.getProperty("hbase_zookeeper_port");
		this.address = Configurations.PROPERTIES.getProperty("hbase_zookeeper");
		init();
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	private void init() {
		configuration = HBaseConfiguration.create(); // 获得配制文件对象
		configuration.set("hbase.zookeeper.property.clientPort", port);
		configuration.set("hbase.zookeeper.quorum", address);
		configuration.set("hbase.client.keyvalue.maxsize", "104857600");
		try {
			connection = ConnectionFactory.createConnection(configuration);// 获得连接对象
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public HBaseClient(Connection conn) {
		setConnection(conn);
	}

	// 获得连接
	private synchronized Connection getConnection() {
		if (connection == null || connection.isClosed()) {
			try {
				connection = ConnectionFactory.createConnection(configuration);
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		}
		return connection;
	}

	private synchronized Configuration getConfiguration() {
		if (configuration == null) {
			try {
				configuration = HBaseConfiguration.create(); // 获得配制文件对象
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
		return configuration;
	}

	public synchronized void setConfiguration(Configuration configuration) {
		if (configuration != null) {
			try {
				this.configuration = configuration;
				this.connection = ConnectionFactory
						.createConnection(configuration);
			} catch (IOException e) {
				logger.error(e);
			}
		} else {
			logger.warn("Configuration is null");
		}
	}

	public synchronized void setConnection(Connection connection) {
		if (connection != null) {
			this.connection = connection;
		}
	}

	// 关闭连接
	public synchronized void close() {
		if (connection != null) {
			try {
				// threadPool.shutdown();
				connection.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	// 创建表
	public void createTable(String tableName, String... FamilyColumn) {
		TableName tn = TableName.valueOf(tableName);
		try {
			Admin admin = getConnection().getAdmin();
			HTableDescriptor htd = new HTableDescriptor(tn);
			for (String fc : FamilyColumn) {
				HColumnDescriptor hcd = new HColumnDescriptor(fc);
				htd.addFamily(hcd);
			}
			admin.createTable(htd);
			admin.close();
		} catch (IOException e) {
			logger.error(e);
		}
	}

	// 删除表
	public void dropTable(String tableName) {
		TableName tn = TableName.valueOf(tableName);
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(tn);
			admin.deleteTable(tn);
			admin.close();
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public void truncateTable(String tableName) {
		TableName tn = TableName.valueOf(tableName);
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(tn);
			admin.truncateTable(tn, false);
			admin.close();
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public boolean tableExists(String tableName) {
		TableName tn = TableName.valueOf(tableName);
		try {
			Admin admin = getConnection().getAdmin();
			Boolean te = admin.tableExists(tn);
			admin.close();
			return te;
		} catch (IOException e) {
			logger.error(e);
		}
		return false;
	}

	public boolean insertList(String tableName, List<HBaseBean> beans) {
		List<Put> puts = new ArrayList<Put>(beans.size());
		int lengths = beans.size();
		for (int i = 0; i < lengths; i++) {
			Put put = new Put(Bytes.toBytes(beans.get(i).getRow()));
			put.addColumn(Bytes.toBytes(beans.get(i).getFamily()),
					Bytes.toBytes(beans.get(i).getColumn()),
					Bytes.toBytes(beans.get(i).getValue()));
			puts.add(put);
		}
		return insertLists(tableName, puts);
	}

	public boolean insertList(String tableName, List<String> rowkeys,
			String family, List<Map<String, Object>> values) {
		List<Put> puts = new ArrayList(rowkeys.size());
		int size = rowkeys.size();
		for (int i = 0; i < size; i++) {
			Put put = new Put(Bytes.toBytes(rowkeys.get(i)));
			Map map = (Map) values.get(i);
			Iterator iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Map.Entry) iterator.next();
				String key = (String) entry.getKey();
				Object value = entry.getValue();
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key),
						Bytes.toBytes(value.toString()));
			}
			puts.add(put);
		}
		return insertLists(tableName, puts);
	}

	public boolean insertList(String tableName, String family,
			List<Map<String, Object>> values) {
		List<Put> puts = new ArrayList<>(values.size());
		int size = values.size();
		for (int i = 0; i < size; i++) {
			Map map = values.get(i);
			String key = (String) map.get(_key);
			Put put = new Put(Bytes.toBytes(key));
			map.remove(_key);
			Iterator iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Map.Entry) iterator.next();
				String keys = (String) entry.getKey();
				Object value = entry.getValue();
				put.addColumn(toBytes(family), toBytes(keys), toBytes(value));
			}
			puts.add(put);
		}
		return insertLists(tableName, puts);
	}

	@Override
	public boolean insert(String tableName, String family,
			Map<String, Object> value) {
		HTable table = (HTable) this.getTable(tableName);
		String rowkey = (String) value.get(_key);
		Put t = new Put(Bytes.toBytes(rowkey));
		Iterator i = value.entrySet().iterator();
		while (i.hasNext()) {
			Map.Entry entry = (Map.Entry) i.next();
			String key = (String) entry.getKey();
			if (!key.equals(_key)) {
				Object val = entry.getValue();
				t.addColumn(Bytes.toBytes(family), Bytes.toBytes(key),
						toBytes(val));
			}
		}
		return put(table, t);
	}

	private boolean insertLists(String tableName, List<Put> list) {
		Table table = this.getTable(tableName);
		try {
			BufferedMutatorParams params = new BufferedMutatorParams(
					TableName.valueOf(tableName));
			params.writeBufferSize(5 * 1024 * 1024);
			BufferedMutator mutator = connection.getBufferedMutator(params);
			mutator.mutate(list);
			mutator.flush();
			return true;
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);

			}
		}
		return false;
	}

	public boolean batchInsertList(final String tableName,
			List<HBaseBean> beans, boolean waiting) {
		final List<Put> puts = new ArrayList<Put>();
		int lengths = beans.size();
		for (int i = 0; i < lengths; i++) {
			Put put = new Put(Bytes.toBytes(beans.get(i).getRow()));
			put.addColumn(Bytes.toBytes(beans.get(i).getFamily()),
					Bytes.toBytes(beans.get(i).getColumn()),
					Bytes.toBytes(beans.get(i).getValue()));
			puts.add(put);
		}

		return batchInsertLists(tableName, puts, waiting);
	}

	public boolean batchInsertLists(final String tableName,
			final List<Put> list, boolean waiting) {
		threadPool
				.execute(() -> {
					try {
						Connection conn = getConnection();
						final BufferedMutator.ExceptionListener listener = (
								RetriesExhaustedWithDetailsException e,
								BufferedMutator mutator) -> {
							for (int i = 0; i < e.getNumExceptions(); i++) {
								logger.error("Failed to sent put "
										+ e.getRow(i) + ".");
							}
						};
						BufferedMutatorParams params = new BufferedMutatorParams(
								TableName.valueOf(tableName))
								.listener(listener);
						params.writeBufferSize(5 * 1024 * 1024);

						final BufferedMutator mutator = conn
								.getBufferedMutator(params);
						try {
							mutator.mutate(list);
							mutator.flush();
						} finally {
							mutator.close();
						}
					} catch (Exception e) {
						logger.error("batchPut failed . ", e);
					}
				});
		if (waiting) {
			try {
				threadPool.awaitTermination();
			} catch (InterruptedException e) {
				logger.error(
						"HBase put job thread pool await termination time out.",
						e);
			}
		}
		return true;
	}

	public boolean batchInsert(String tableName, Put put, boolean waiting) {
		return batchInsertLists(tableName, Arrays.asList(put), waiting);
	}

	public boolean batchInsert(String tableName, HBaseBean bean, boolean waiting) {
		Put put = new Put(Bytes.toBytes(bean.getRow()));
		put.addColumn(Bytes.toBytes(bean.getFamily()),
				Bytes.toBytes(bean.getColumn()), Bytes.toBytes(bean.getValue()));
		return batchInsert(tableName, put, waiting);
	}

	public Table getTable(String tableName) {
		try {
			return getConnection().getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error(e);
		}
		return null;
	}

	// 插入或者更新数据
	public boolean insert(String tableName, String rowKey, String family,
			String qualifier, String value) {
		return insert(tableName, rowKey, family, new String[] { qualifier },
				new String[] { value });
	}

	public boolean insert(String tableName, String rowKey, String family,
			String[] qualifiers, String[] values) {
		HTable t = (HTable) this.getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		int i = qualifiers.length;
		for (int j = 0; j < i; j++) {
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifiers[j]),
					Bytes.toBytes(values[j]));
		}
		return put(t, put);
	}

	public boolean insert(String tableName, String rowKey, String family,
			Map values) {
		HTable t = (HTable) this.getTable(tableName);
		Put put = new Put(toBytes(rowKey));
		Iterator iterator = values.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, String> entrys = (Map.Entry) iterator.next();
			String key = entrys.getKey();
			Object value = entrys.getValue();
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key),
					toBytes(value));
		}
		return put(t, put);
	}

	@Override
	public boolean insert(String tableName, List<String> rowkey, String family,
			Map<String, Object> values) {
		HTable t = (HTable) this.getTable(tableName);
		int size = rowkey.size();
		List<Put> puts = new ArrayList();
		for (int i = 0; i < rowkey.size(); i++) {
			Put put = new Put(toBytes(rowkey.get(i)));
			Iterator iterator = values.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, String> entrys = (Map.Entry) iterator.next();
				String key = entrys.getKey();
				Object value = entrys.getValue();
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key),
						toBytes(value));
			}
			puts.add(put);
		}
		return put(t, puts);
	}

	public boolean checkAndPut(String tableName, String rowkey, String family,
			String qualifier, String newValue, String oldValue) {
		HTable t = (HTable) this.getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				Bytes.toBytes(newValue));
		boolean flag = false;
		try {

			flag = t.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes(family),
					toBytes(qualifier), toBytes(oldValue), put);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return flag;
	}

	public boolean insert(String tableName, String rowKey, String family,
			String qualifier, byte[] value) {
		HTable t = (HTable) this.getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
		return put(t, put);
	}

	// 删除
	public boolean delete(String tableName, String rowKey, String family,
			String qualifier) {
		Table t = this.getTable(tableName);
		Delete del = new Delete(Bytes.toBytes(rowKey));
		try {
			if (qualifier != null) {
				del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			} else if (family != null) {
				del.addFamily(Bytes.toBytes(family));
			}
			t.delete(del);
			return true;
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return false;
	}

	// 删除一行
	public boolean delete(String tableName, String rowKey) {
		return delete(tableName, rowKey, null, null);
	}

	// 删除一行下的一个列族
	public boolean delete(String tableName, String rowKey, String family) {
		return delete(tableName, rowKey, family, null);
	}

	@Override
	public boolean deletes(String tableName, String family, List<String> rows,
			List<String> qualifer) {
		HTable table = (HTable) this.getTable(tableName);
		List<Delete> deletes = new ArrayList();
		int size = rows.size();
		int qualifers = qualifer.size();
		for (int i = 0; i < size; i++) {
			Delete delete = new Delete(Bytes.toBytes(rows.get(i)));
			for (int j = 0; j < qualifers; j++) {
				delete.addColumn(toBytes(family), toBytes(qualifer.get(j)));
			}
			deletes.add(delete);
		}
		return deleteList(table, deletes);
	}

	public boolean deleteList(String tableName, String family, String rows,
			List<String> cloums) {
		return deletes(tableName, family, Arrays.asList(rows), cloums);
	}

	public byte[] byGetBytes(String tableName, String rowKey, String family,
			String qualifier) {
		Table t = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		try {
			Result r = t.get(get);
			return CellUtil.cloneValue(r.listCells().get(0));
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return null;
	}

	// 数据读取
	// 取到一个值
	public String byGet(String tableName, String rowKey, String family,
			String qualifier) {
		return Bytes.toString(byGetBytes(tableName, rowKey, family, qualifier));
	}

	// 取到一个族列的值
	public Map<String, String> byGet(String tableName, String rowKey,
			String family) {
		Map<String, String> result = null;
		Table t = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		try {
			Result r = t.get(get);
			List<Cell> cs = r.listCells();
			result = cs.size() > 0 ? new HashMap<String, String>() : result;
			for (Cell cell : cs) {
				result.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)));
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e);
			}
			// close();
		}

		return result;
	}

	// 取到多个族列的值
	public Map<String, Map<String, String>> byGet(String tableName,
			String rowKey) {
		Map<String, Map<String, String>> results = null;
		Table t = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));

		try {
			Result r = t.get(get);
			List<Cell> cs = r.listCells();
			results = cs.size() > 0 ? new HashMap<String, Map<String, String>>()
					: results;
			for (Cell cell : cs) {
				String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
				if (results.get(familyName) == null) {
					results.put(familyName, new HashMap<String, String>());
				}
				results.get(familyName).put(
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)));
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return results;
	}

	public List getAlls(String tableName, String startRowkey, String endRowkey) {
		List<JSONObject> result = new ArrayList();
		Table table = getTable(tableName);
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowkey));
		scan.setStopRow(Bytes.toBytes(endRowkey));
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
			for (Result rs : scanner) {
				List<JSONObject> parseResult = parseResult(rs);
				result.addAll(parseResult);
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return result;
	}

	@Override
	public Map<String, Map<String, String>> getAll(String tableName,
			String startRowkey, String endRowkey) {
		Map<String, Map<String, String>> map = new HashMap<>();
		Table table = getTable(tableName);
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowkey));
		scan.setStopRow(Bytes.toBytes(endRowkey));
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
			for (Result rs : scanner) {
				Map<String, Map<String, String>> parseResult = parseResulta(rs);
				map.putAll(parseResult);
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return map;
	}

	public void batchShutDown() {
		threadPool.shutdown();
	}

	public void awaitTermination() {
		try {
			threadPool.awaitTermination();
		} catch (InterruptedException e) {
			logger.error(e);
		}
	}

	private List<JSONObject> parseResult(Result result) {
		List list = new ArrayList();
		JSONObject json = new JSONObject();
		if (result.isEmpty()) {
			logger.warn("未查询到数据！");
			return list;
		}
		for (Cell cell : result.listCells()) {
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			json.put(qualifier, value);
		}
		list.add(json);
		return list;
	}

	private Map<String, Map<String, String>> parseResulta(Result result) {
		Map<String, Map<String, String>> map = new HashMap<>();

		if (result.isEmpty()) {
			logger.warn("未查询到数据！");
			return map;
		}
		String rowkey = Bytes.toString(result.getRow());
		Map json = new HashMap();
		for (Cell cell : result.listCells()) {
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
			json.put(qualifier, value);
		}
		map.put(rowkey, json);
		return map;
	}

	public List getAllRows(String tableName) {
		List<JSONObject> results = new ArrayList();
		Table talbe = this.getTable(tableName);
		Scan scan = new Scan();
		ResultScanner results1 = null;
		try {
			results1 = talbe.getScanner(scan);
		} catch (IOException e) {
			logger.error(e);
		}

		for (Result r : results1) {
			results.addAll(parseResult(r));
		}
		return results;
	}

	public Map getAllRowsA(String tableName) {

		Table talbe = this.getTable(tableName);
		Scan scan = new Scan();
		ResultScanner results1 = null;
		try {
			results1 = talbe.getScanner(scan);

		} catch (IOException e) {
			logger.error(e);
		}
		Map results = new HashMap(58118);
		for (Result r : results1) {
			results.putAll(parseResulta(r));
		}
		return results;
	}

	public Map get(String tableName, List<String> rows) {
		// TODO Auto-generated method stub
		Map<String, List<JSONObject>> map = new HashMap();
		Table table = getTable(tableName);
		List<Get> gets = new ArrayList();
		for (String row : rows) {
			Get get = new Get(Bytes.toBytes(row));
			gets.add(get);
		}
		try {
			Result[] results = table.get(gets);
			for (int i = 0; i < results.length; i++) {
				List<JSONObject> list = parseResult(results[i]);
				map.put(rows.get(i), list);
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return map;
	}

	public List<Map<String, String>> get(String tableName, List<String> rows,
			String family, List<String> cloums) {
		List<Map<String, String>> result = new ArrayList();
		Table table = getTable(tableName);
		List<Get> gets = new ArrayList();
		for (String row : rows) {
			Get get = new Get(Bytes.toBytes(row));
			for (String cloum : cloums) {
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(cloum));
			}
			gets.add(get);
		}
		try {
			Result[] results = table.get(gets);
			for (int i = 0; i < results.length; i++) {
				List<Cell> cells = results[i].listCells();
				Map<String, String> map = new HashMap<>();
				if (cells != null) {
					for (Cell cell : cells) {
						String value = Bytes
								.toString(CellUtil.cloneValue(cell));
						String qualifier = Bytes.toString(CellUtil
								.cloneQualifier(cell));
						map.put(qualifier, value);
					}
				}
				result.add(map);
			}
		} catch (IOException e) {
			logger.error(e);
		}
		return result;
	}

	public List get(String tableName, Date startDate, Date endDate) {
		List<JSONObject> result = new ArrayList();

		Table table = getTable(tableName);
		Scan scan = new Scan();

		Date start = null, end = null;
		start = startDate;
		end = endDate;

		scan.setStartRow(Bytes.toBytes(start.getTime()));
		scan.setStopRow(Bytes.toBytes(end.getTime()));

		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
			for (Result rs : scanner) {
				List<JSONObject> parseResult = parseResult(rs);
				result.addAll(parseResult);
			}
		} catch (IOException e) {
			logger.error(e);
		} finally {
			scanner.close();
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}

		return result;
	}

	public List get(String tableName, String rowkey) {
		Table table = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowkey));
		Result result = null;
		List<JSONObject> list = null;
		try {
			result = table.get(get);
			list = parseResult(result);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}

		return list;

	}

	public List get(String tableName, String rowKey, String family) {
		Table t = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		Result result = null;
		try {
			result = t.get(get);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return parseResult(result);
	}

	public HBaseBean get(String tableName, String rowKey, String family,
			String qualifier) {
		Table t = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		try {

			Result r = t.get(get);
			String value = Bytes.toString(CellUtil.cloneValue(r.listCells()
					.get(0)));
			long timestamp = r.listCells().get(0).getTimestamp();
			return new HBaseBean(rowKey, family, qualifier, value, timestamp);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				t.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return null;
	}

	public Map<String, Object> get(String tableName, String rowkey,
			String family, List<String> qualifiers) {
		Table table = this.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowkey));
		for (String column : qualifiers) {
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		}
		Result result;
		try {
			result = table.get(get);
			List<Cell> cells = result.listCells();
			Map<String, Object> results = new JSONObject();
			for (String str : qualifiers) {
				results.put(str, "");
			}
			if (cells != null) {
				for (Cell cell : cells) {
					String qualifier = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					byte[] val = CellUtil.cloneValue(cell);
					results.put(qualifier, isIn(qualifier, val));
				}
			}
			return results;
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return null;
	}

	public void appendData(String tableName, String rowkey, String family,
			String qualifier, String value) {
		Table table = this.getTable(tableName);
		Append append = new Append(Bytes.toBytes(tableName));
		append.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				Bytes.toBytes(value));
		try {
			table.append(append);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	public void delMultiRows(String tableName, String[] rowKeys) {
		Table table = this.getTable(tableName);
		List<Delete> delList = new ArrayList();
		for (String row : rowKeys) {
			Delete del = new Delete(Bytes.toBytes(row));
			delList.add(del);
		}
		try {
			table.delete(delList);
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	private byte[] toBytes(String value) {
		return Bytes.toBytes(value);
	}

	private boolean put(Table table, Put put) {
		List<Put> puts = new ArrayList<>();
		puts.add(put);
		return put(table, puts);
	}

	private boolean put(Table table, List<Put> puts) {
		
		try {
			BufferedMutatorParams params = new BufferedMutatorParams(table.getName());
			params.writeBufferSize(5 * 1024 * 1024);
			BufferedMutator mutator = connection.getBufferedMutator(params);
			mutator.mutate(puts);
			mutator.flush();
			
			return true;
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return false;
	}

	private boolean deleteList(HTable table, List<Delete> delete) {
		try {
			BufferedMutatorParams params = new BufferedMutatorParams(table.getName());
			params.writeBufferSize(5 * 1024 * 1024);
			BufferedMutator mutator = connection.getBufferedMutator(params);
			mutator.mutate(delete);
			mutator.flush();
			return true;
		} catch (IOException e) {
			logger.error(e);
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		return false;
	}

	private byte[] toBytes(Object o) {
		if (o instanceof Integer) {
			return PhTypeUtil.toBytes(o, PhType.INTEGER);
		}
		if (o instanceof Long) {
			return PhTypeUtil.toBytes(o, PhType.BIGINT);
		}
		if (o instanceof Short) {
			return PhTypeUtil.toBytes(o, PhType.SMAILLINT);
		}
		if (o instanceof Float) {
			return PhTypeUtil.toBytes(o, PhType.FLOAT);
		}
		if (o instanceof Double) {
			return PhTypeUtil.toBytes(o, PhType.DOUBLE);
		}
		if (o instanceof BigDecimal) {
			return PhTypeUtil.toBytes(o, PhType.DECIMAL);
		}

		String object = o.toString();
		return PhTypeUtil.toBytes(object, PhType.VARCHAR);
	}

	private Object isIn(String key, byte[] bytes) {
		if (longString.contains(key)) {
			Long val = (Long) PhTypeUtil.toObject(bytes, PhType.BIGINT);
			return val.toString();
		}
		if (doubleString.contains(key)) {
			Double val = (Double) PhTypeUtil.toObject(bytes, PhType.DOUBLE);
			return val.toString();
		}
		return Bytes.toString(bytes);
	}
}
