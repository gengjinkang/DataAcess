package com.fuhao.data.datasource;

import java.io.Closeable;
import java.util.Map;

public interface DataSource extends Closeable,DataSourceOperation{

    /**
     * 开启事务
     */
    void beginTransaction();


    /**
     * 返回数据源的名字
     * @return
     */
    String getSourceName();

    /**
     * 返回这个数据源下的字段信息
     * @return  {"字段":{信息}}
     */
    Map<String,Map<String,String>> getInfo();

    /**
     * 提交事务
     */
    void commit() throws Exception;

    /**
     * 回滚
     */
    void rollbcak();



    void closeSource();

}
