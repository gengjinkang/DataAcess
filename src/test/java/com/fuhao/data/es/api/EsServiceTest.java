package com.fuhao.data.es.api;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import com.fuhao.data.fhbase.HBaseClient;

public class EsServiceTest {

	@Test
	public void testEs() {
		ElasticSearchService service = new ElasticSearchService(
				"192.168.1.101", 9200, "http");

	}

	@Test
	public void testgetAllList() {
		ElasticSearchService service = new ElasticSearchService(
				"192.168.1.101", 9200, "http");

		HBaseClient client = new HBaseClient();
		Map all = client.getAllRowsA("gisdata");
		// service.bulkInsertDataByMap(index, type, idWithJson);
	}

	@Test
	public void testHBaseClient() throws IOException {
		ElasticSearchService service = new ElasticSearchService(
				"192.168.1.101", 9200, "http");

		HBaseClient client = new HBaseClient();
		Map all = client.getAllRowsA("gisdata");
		// service.bulkInsertDataByMap("gisdata", "gd", all);
		int count = 0;
		Set entrySet = all.entrySet();
		Map map = new HashMap();
		for (Object object : entrySet) {
			Entry entry = (Entry) object;
			map.put(entry.getKey(), entry.getValue());
			count++;
			if (count % 1024 == 0) {
				service.bulkInsertDataByMap("gisdata", "gd", map);
				map = new HashMap();
			}
		}
		if (map.size() != 0) {
			service.bulkInsertDataByMap("gisdata", "gd", map);
		}
		System.out.println(count);
		System.out.println(all.size());
		// service.bulkInsertDataByMap("text", "text", all);
		// service.close();
		client.close();
	}

	private void writeToFile(int index, Map map) {

		try {
			FileOutputStream fos = new FileOutputStream(new File("H://repo/map"
					+ index));
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(map);
			oos.close();
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testESHighRequest() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("hadoop", 9200, "http"),
						new HttpHost("slave2", 9200, "http")));
		IndexRequest request=new IndexRequest("fh","people");
		Map map=new HashMap();
		map.put("sno", 1001);
		map.put("name", "tom");
		request.source(map);
		try {
			IndexResponse index = client.index(request);
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void testSplit() {

	}
}
