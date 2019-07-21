package com.fuhao.data.datasource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fuhao.data.datasource.api.PhoenixDataSource;
import com.fuhao.data.datasource.api.SqlDataSource;


public class SourceMananger implements DataSourceManager {
	
    private Map<String,DataSource> sources;
    public SourceMananger() {
    	sources=new HashMap<String,DataSource>();
    	addDataSource(new PhoenixDataSource());
    	addDataSource(new SqlDataSource());
    }
    @Override
    public Map<String, Map<String, String>> getAllInfo() {
        Map<String, Map<String, String>> result = new HashMap<>();
        for(DataSource client:sources.values()){
            Map<String, Map<String, String>> info = client.getInfo();
            result.putAll(info);
        }
        return result;
    }

    @Override
    public void addDataSource(DataSource client) {
        if(client==null){
            return;
        }
        sources.put(client.getSourceName(),client);
    }

    @Override
    public void removeDataSource(String sourceName) {
        if(sourceName==null||"".equals(sourceName)){
            return;
        }
        sources.remove(sourceName);
    }

    @Override
    public DataSource getDataClient(String sourceName) {
        return this.sources.get(sourceName);
    }

    @Override
    public void close() throws IOException {
       for(DataSource client:sources.values()){
           client.close();
       }
    }


}
