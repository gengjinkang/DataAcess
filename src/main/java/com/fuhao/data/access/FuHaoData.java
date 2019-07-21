package com.fuhao.data.access;

/**
 * FuHao数据访问接口
 * @author lz
 * 2018.1.18
 */



import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public interface FuHaoData {

    /**
     * 事务的开启
     */
    void beginTransaction();

    /**
     * 存入多行数据的方法
     * @param datas  [{'字段':数据，'字段':数据}]  字段名字必须是String类型，数据必须是Object,会调用拿到的数据的toString方法存储
     * @return
     */
    boolean put(JSONArray datas) throws Exception;

    boolean put(JSONArray datas,String tableName) throws Exception;


    /**
     * 存入一行数据的方法
     * 定义如上
     * @param data
     * @return
     */
    boolean put(JSONObject data) throws Exception;

    boolean put(JSONObject data,String tableName) throws Exception;

    /**
     * 返回相应的索引，可通过索引拿数据
     * @param datas     返回多行RowKey
     * @return
     */
    JSONArray putReturnKeys(JSONArray datas)throws Exception;


    JSONArray putReturnKeys(JSONArray datas,String tableName)throws Exception;
    /**
     * 返回相应的索引，可通过索引拿数据
     * @param data    返回一行RowKey
     * @return
     */
    String putReturnKey(JSONObject data)throws Exception;

    String putReturnKey(JSONObject data,String tableName)throws Exception;
    /**
     * @param data  {'字段':数据}
     * @param condition 条件   'id >1 and name ='lz' or money >1000' 和Sql条件一样
     * @return
     */
    boolean updateWithCondition(JSONObject data, String condition)throws Exception;

    boolean updateWithCondition(JSONObject data, String condition,String tableName)throws Exception;

    boolean updateWithCondition(JSONObject data, String condition,JSONArray datas,String tableName)throws Exception;

    /**
     * 通过索引更新
     * @param data {'字段':数据}
     * @param _keys 索引
     * @return
     */
    boolean updateWithRowKeys(JSONObject data, JSONArray _keys) throws Exception;

    boolean updateWithRowKeys(JSONObject data, JSONArray _keys,String tableName) throws Exception;

    boolean updateWithRowKey(JSONObject data, String _key) throws Exception;

    boolean updateWithRowKey(JSONObject data, String _key,String tableName) throws Exception;

    /**
     * 查询数据
     * @param fields  查询的字段 ['字段名','字段名']
     * @param condition     条件
     * @return
     */

    JSONArray get(JSONArray fields, String condition) throws Exception;

    JSONArray get(JSONArray fields, String condition,String tableName) throws Exception;

    JSONArray get(JSONArray fields, String condition,JSONArray datas,String tableName) throws Exception;

    JSONArray get(JSONArray fields, JSONArray _key) throws Exception;

    JSONArray get(JSONArray fields, JSONArray _key,String tableName) throws Exception;

    JSONObject getByKey(JSONArray fields, String _key) throws Exception;

    JSONObject getByKey(JSONArray fields, String _key,String tableName) throws Exception;

    /**
     * 删除数据
     * @param fields    删除的字段
     * @param _keys     通过索引删除或者条件删除
     * @return
     */

    boolean delete(JSONArray fields, JSONArray _keys) throws Exception;

    boolean delete(JSONArray fields, JSONArray _keys,String tableName) throws Exception;

    boolean delete(JSONArray fields, String condition) throws Exception;

    boolean delete(JSONArray fields, String condition,String tableName) throws Exception;

    boolean delete(JSONArray fields, String condition,JSONArray datas,String tableName) throws Exception;

    /**
     * 对指定字段进行增加数据 Check And Put 避免出现脏读
     * @param rowkey
     * @param qualifer
     * @param add
     * @return
     */

    boolean CAPadd(String rowkey, String qualifer, JSONArray add) throws Exception;

    boolean CAPadd(String rowkey, String qualifer, JSONArray add,String tableName) throws Exception;

    boolean CAPadd(String rowkey, String qualifer, String add) throws Exception;

    boolean CAPadd(String rowkey, String qualifer, String add,String tableName) throws Exception;
    /**
     * 对指定字段进行移除数据 Check And Put 避免出现脏读
     * @param rowkey
     * @param qualifer
     * @param remove
     * @return
     */

    boolean CAPremove(String rowkey, String qualifer, JSONArray remove);

    boolean CAPremove(String rowkey, String qualifer, JSONArray remove,String tableName);

    boolean CAPremove(String rowkey, String qualifer, String remove);

    boolean CAPremove(String rowkey, String qualifer, String remove,String tableName);

    /**
     * 提交事务
     */

    void commit() throws Exception;

    /**
     * 释放资源
     */
    void close() throws IOException;
}
