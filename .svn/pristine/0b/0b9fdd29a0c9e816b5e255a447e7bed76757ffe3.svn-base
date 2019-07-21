package com.fuhao.data.datasource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.sql.SQLException;

public interface DataSourceOperation {

    boolean put(JSONArray datas,String tableName) throws Exception;
    /**
     * 存入一行数据的方法
     * 定义如上
     * @param data
     * @return
     */
    boolean put(JSONObject data,String tableName)throws Exception;

    /**
     * @param data  {'字段':数据}
     * @param condition 条件   'id >1 and name ='lz' or money >1000' 和Sql条件一样
     * @return
     * @throws SQLException 
     */
    boolean updateWithCondition(JSONObject data, String condition,String tableName) throws Exception;

    boolean updateWithCondition(JSONObject data, String condition,JSONArray datas,String tableName)throws Exception;

    /**
     * 通过索引更新
     * @param data {'字段':数据}
     * @param _keys 索引
     * @return
     */
    boolean updateWithRowKeys(JSONObject data, JSONArray _keys,String tableName) throws Exception;

    boolean updateWithRowKey(JSONObject data, String _key,String tableName) throws Exception;

    /**
     * 查询数据
     * @param fields  查询的字段 ['字段名','字段名']
     * @param condition     条件
     * @return
     * @throws SQLException 
     */

    JSONArray get(JSONArray fields, String condition,String tableName) throws Exception;

    JSONArray get(JSONArray fields, JSONArray _key,String tableName) throws Exception;

    JSONObject getByKey(JSONArray fields, String _key,String tableName) throws Exception;

    JSONArray get(JSONArray fields, String condition,JSONArray datas,String tableName) throws Exception;
    /**
     * 删除数据
     * @param fields    删除的字段
     * @param _keys     通过索引删除或者条件删除
     * @return
     */

    boolean deleteKeys(JSONArray fields, JSONArray _keys,String tableName)throws Exception;

    boolean deleteKey(JSONArray fields, String key,String tableName) throws Exception;

    boolean delete(JSONArray fields, String condition,String tableName) throws Exception;

    boolean delete(JSONArray fields, JSONArray _keys,String tableName) throws Exception;

    boolean delete(JSONArray fields, String condition,JSONArray datas,String tableName) throws Exception;
    /**
     * 对指定字段进行增加数据 Check And Put 避免出现脏读
     * @param rowkey
     * @param qualifer
     * @param add
     * @return
     */

    boolean CAPadd(String rowkey, String qualifer, JSONArray add,String tableName) throws Exception;

    boolean CAPadd(String rowkey, String qualifer, String add,String tableName) throws Exception;
    /**
     * 对指定字段进行移除数据 Check And Put 避免出现脏读
     * @param rowkey
     * @param qualifer
     * @param remove
     * @return
     */

    boolean CAPremove(String rowkey, String qualifer, JSONArray remove,String tableName);

    boolean CAPremove(String rowkey, String qualifer, String remove,String tableName);
}
