package com.fuhao.data.util;

import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class PhoenixSqlUtil {
    /**
     * 将给出的字符串尾部的逗号去掉
     *
     * @param oldStr 需要修改的字符串
     * @return 返回一个不是以逗号结尾的字符串
     */
    private static StringBuffer deleteEnd(StringBuffer oldStr) {
        int len = oldStr.length();
        if (oldStr.lastIndexOf(",") == len - 1) {
            oldStr = new StringBuffer(oldStr.substring(0, len - 1));
        }
        return oldStr;
    }

    /**
     * 将传入的json和tableName解析为SQL数据库的插入语句。
     *
     * @param json      要插入的数据
     * @param tableName 表名称
     * @return {@link String}返回插入语句
     */
    public static String parseToInsertSQL(JSONObject json, String tableName, JSONObject types) {
        Iterator iterator = json.keys();
        StringBuffer insertSQL = new StringBuffer();
        insertSQL.append("UPSERT INTO ").append("\"" + tableName + "\"")
                .append("(");
        StringBuffer placeHolder = new StringBuffer();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            if (types == null) {
                insertSQL.append("\"" + key + "\",");
            } else {
                if ("_key".equals(key)) {
                    insertSQL.append("\"" + key + "\",");
                } else {
                    String type = types.getString(key);
                    insertSQL.append("\"" + key + "\" " + type + ",");
                }
            }
            placeHolder.append("?,");
        }
        insertSQL = deleteEnd(insertSQL);
        insertSQL.append(") VALUES(").append(deleteEnd(placeHolder))
                .append(")");
        return insertSQL.toString();
    }


    public static String parseToQuerySQL(JSONArray fields, String condition,
                                         String tableName, JSONObject types) {
        int len = fields.size();
        StringBuffer querySQL = new StringBuffer("SELECT ");
        for (int i = 0; i < len; i++) {
            querySQL.append("\"" + fields.get(i) + "\",");
        }
        querySQL = deleteEnd(querySQL);
        querySQL.append(" FROM ").append("\"" + tableName + "\"");
        if (types != null) {
            for (int i = 0; i < len; i++) {
                String key = fields.getString(i);
                if (i == 0) {
                    querySQL.append("(");
                }

                if (!key.equals("_key")) {
                    String type = types.getString(key);
                    querySQL.append("\"" + key + "\" " + type + ",");
                }
                if (i == len - 1) {
                    querySQL.delete(querySQL.length() - 1, querySQL.length());
                    querySQL.append(")");
                }
            }
        }
        querySQL.append(" ");
        querySQL.append(condition);
        return querySQL.toString();
    }


    public static String parseToDeleteSQL(String condition, String tableName) {
        StringBuffer deleteSQL=new StringBuffer();
        deleteSQL.append("DELETE FROM ").append(tableName).append(" ").append(condition);
        return deleteSQL.toString();
    }


}
