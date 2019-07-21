package com.fuhao.data.datasource;

import java.io.IOException;
import java.util.Map;

public interface DataSourceManager {

    /**
     * 获取数据源下所有字段的信息
     * @return
     */
    Map<String,Map<String,String>> getAllInfo();

    /**
     * 添加数据源
     * @param dataSource
     */

    void addDataSource(DataSource dataSource);

    /**
     * 移除数据源
     * @param sourceName
     */
    void removeDataSource(String sourceName);

    /**
     * 获得制定数据源
     * @param sourceName
     * @return
     */
    DataSource getDataClient(String sourceName);

    /**
     * 关闭资源
     * @throws IOException
     */
    void close() throws IOException;
}
