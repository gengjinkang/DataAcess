package com.fuhao.data.fhbase.inter;

import com.fuhao.data.fhbase.HBaseBean;

import java.util.List;
import java.util.Map;

public interface Puts {
    /**
     * 多线程插入
     * @param tableName
     * @param list
     * @param waiting
     * @return
     */
    boolean batchInsertList(String tableName, List<HBaseBean> list, boolean waiting);

    boolean batchInsert(String tableName, HBaseBean baseBean, boolean waiting);

    /**
     * 插入数据
     * @param tableName
     * @param list
     * @return
     */
    boolean insertList(String tableName, List<HBaseBean> list);

    boolean insertList(String tableName, List<String> rowkeys, String family, List<Map<String,Object>> values);

    boolean insertList(String tableName,String family,List<Map<String,Object>> values);

    boolean insert(String tableName, String family,Map<String,Object> value);

    boolean insert(String tableName, String rowKey, String family, String qualifier, String value);

    boolean insert(String tableName, String rowKey, String family, String[] qualifiers, String[] values);

    boolean insert(String tableName, String rowKey, String family, String qualifier, byte[] value);

    boolean insert(String tableName, String rowKey, String family, Map<String,Object> values);

    boolean insert(String tableName, List<String> rowkey, String family, Map<String,Object> values);

    boolean checkAndPut(String tableName, String rowkey, String family, String qualifier, String newValue, String oldValue);
}
