package com.fuhao.data.es.api;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;

public class ElasticSearchService {

    private final static int MAX=10000;

    private RestHighLevelClient client;

    public ElasticSearchService(String ip,int port,String scheme){
        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ip, port, scheme)));
    }

    public ElasticSearchService(HttpHost... hosts){
        this.client = new RestHighLevelClient(
                RestClient.builder(hosts));
    }

    public void createIndex(String indexName){
        CreateIndexRequest request =
                new CreateIndexRequest(indexName);

        try {
            this.client.indices().create(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteIndex(String indexName){
        DeleteIndexRequest request =
                new DeleteIndexRequest(indexName);
        try {
            this.client.indices().delete(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void openIndex(String indexName){
        OpenIndexRequest request =
                new OpenIndexRequest(indexName);
        try {
           this.client.indices().open(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String index,String type,String json){
        IndexRequest request =
                new IndexRequest(index,type);
        request.source(json);
        try {
            IndexResponse indexResponse = this.client.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void insertData(String index,String type,Map<String,Object> json){
        IndexRequest request =
                new IndexRequest(index,type);
        request.source(json);
        try {
            IndexResponse indexResponse = this.client.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void insertData(String index,String type,String id,String json){
        IndexRequest request =
                new IndexRequest(index,type,id);
        request.source(json);
        try {
            IndexResponse indexResponse = this.client.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String index,String type,String id,Map<String,Object> values){
        IndexRequest request =
                new IndexRequest(index,type,id);
        request.source(values);
        try {
            IndexResponse indexResponse = this.client.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateData(String index,String type,String id,String json){
        UpdateRequest updateRequest =
                new UpdateRequest(index,type,id);
        updateRequest.doc(json);

        try {
            this.client.update(updateRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateData(String index,String type,String id,Map<String,Object> json){
        UpdateRequest updateRequest =
                new UpdateRequest(index,type,id);
        updateRequest.doc(json);

        try {
            this.client.update(updateRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void bulkInsertDataByString(String index,String type,List<String> jsonList){
        BulkRequest request =
                new BulkRequest();
        for(String value : jsonList){
            IndexRequest indexRequest =
                    new IndexRequest(index,type);
            indexRequest.source(value);
            request.add(indexRequest);
        }
        try {
            this.client.bulk(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void bulkInsertDataByMap(String index,String type,List<Map> jsonList){
        BulkRequest request =
                new BulkRequest();
        for(Map value : jsonList){
            IndexRequest indexRequest =
                    new IndexRequest(index,type);
            indexRequest.source(value);
            request.add(indexRequest);
        }
        try {
            this.client.bulk(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void bulkInsertDataByMap(String index,String type,Map<String,Map<String,String>> idWithJson){
        BulkRequest request =
                new BulkRequest();
        Iterator i = idWithJson.entrySet().iterator();

        while(i.hasNext()){
            Map.Entry<String,Map<String,String>> entry = (Map.Entry) i.next();
            String key = entry.getKey();
            Map<String,String> value = entry.getValue();
            IndexRequest indexRequest =
                    new IndexRequest(index,type,key);
            indexRequest.source(value);
            request.add(indexRequest);
        }
        try {
            BulkResponse response = this.client.bulk(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public List<Map<String,Object>> search(String index,String type,EsQueryBuilderConstructor constructor){
        List<Map<String,Object>> result = new ArrayList<>();
        SearchRequest searchRequest =
                new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (StringUtils.isNotEmpty(constructor.getAsc()))
        { searchSourceBuilder.sort(constructor.getAsc(), SortOrder.ASC);}
        if (StringUtils.isNotEmpty(constructor.getDesc()))
        {   searchSourceBuilder.sort(constructor.getDesc(), SortOrder.DESC);}
        int size = constructor.size;
        if(size<0){
            size=0;
        }
        if(size>MAX){
            size = MAX;
        }
        searchSourceBuilder.size(size);

        searchSourceBuilder.from(constructor.getFrom() < 0 ? 0 : constructor.getFrom());

        searchSourceBuilder.query(constructor.listBuilders());

        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse response = this.client.search(searchRequest);
           SearchHits hits =  response.getHits();
            SearchHit[] searchHists = hits.getHits();
            for (SearchHit sh : searchHists) {

                result.add(sh.getSourceAsMap());
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String,Map<String,Object>> searchWithRk(String index,String type,EsQueryBuilderConstructor constructor){
        Map<String,Map<String,Object>> result = new HashMap<>();
        SearchRequest searchRequest =
                new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (StringUtils.isNotEmpty(constructor.getAsc()))
        { searchSourceBuilder.sort(constructor.getAsc(), SortOrder.ASC);}
        if (StringUtils.isNotEmpty(constructor.getDesc()))
        {   searchSourceBuilder.sort(constructor.getDesc(), SortOrder.DESC);}
        int size = constructor.size;
        if(size<0){
            size=0;
        }
        if(size>MAX){
            size = MAX;
        }
        searchSourceBuilder.size(size);

        searchSourceBuilder.from(constructor.getFrom() < 0 ? 0 : constructor.getFrom());

        searchSourceBuilder.query(constructor.listBuilders());

        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse response = this.client.search(searchRequest);
            SearchHits hits =  response.getHits();
            SearchHit[] searchHists = hits.getHits();
            for (SearchHit sh : searchHists) {
                        String id = sh.getId();
                        Map map = sh.getSourceAsMap();
                        result.put(id,map);
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public void close(){
        try {
            this.client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
