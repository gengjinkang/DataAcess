package com.fuhao.data.datasource.api;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class EsSearchTest {
	public static void main(String[] args) {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("hadoop", 9200, "http"),
						new HttpHost("slave2", 9200, "http")));
	}
}
