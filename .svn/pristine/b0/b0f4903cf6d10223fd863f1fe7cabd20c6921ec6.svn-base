package com.fuhao.data.fhbase.inter;

import com.fuhao.data.fhbase.HBaseBean;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Gets {
    /**
     * 读取操作
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param qualifier 列
     * @return          值
     */

    byte[] byGetBytes(String tableName, String rowKey, String family, String qualifier);

    String byGet(String tableName, String rowKey, String family, String qualifier) ;

    Map<String, String> byGet(String tableName, String rowKey, String family) ;

    Map<String, Map<String, String>> byGet(String tableName, String rowKey) ;

    List<Map<String, String>> get(String tableName, String rowKey) ;

    List<Map<String, String>> get(String tableName, String rowKey, String family);

    HBaseBean get(String tableName, String rowKey, String family, String qualifier);


    /**
     * 追加数据
     * @param tableName 表名字
     * @param rowkey    主键
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    void appendData(String tableName, String rowkey, String family, String qualifier, String value);

    void delMultiRows(String tableName, String[] rowKeys) ;

    List<Map<String, String>> getAllRows(String tableName) ;

    Map<String, List<Map<String, String>>> get(String tableName, List<String> rows) ;

    List<Map<String,String>> get(String tableName, List<String> rows, String family, List<String> cloums);

    Map<String,Object> get(String tableName, String rows, String family, List<String> cloums);

    List<Map<String, String>> get(String tableName, Date startDate, Date endDate) ;

    List<Map<String, String>> getAlls(String tableName, String startRowkey, String endRowkey) ;

    Map<String,Map<String,String>> getAll(String tableName, String startRowkey, String endRowkey);
}
