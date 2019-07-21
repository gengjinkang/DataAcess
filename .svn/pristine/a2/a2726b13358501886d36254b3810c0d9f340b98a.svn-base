package com.fuhao.data.util;

public class DataFormat {

    public static Object format(String type,String value){
        if ("INTEGER".equals(type)){
            return Integer.parseInt(value);
        } if ("BIGINT".equals(type)){
            return Long.parseLong(value);
        }if ("FLOAT".equals(type)){
            return Float.parseFloat(value);
        }if ("DOUBLE".equals(type)){
            return Double.parseDouble(value);
        }if("DATE".equals(type)) {
        	return DateFormatUtil.toDate(value);
        }if("TIME".equals(type)) {
        	return DateFormatUtil.toTime(value);
        }if("TIMESTAMP".equals(type)) {
        	return DateFormatUtil.toTimestamp(value);
        }if("DATETIME".equals(type)) {
        	return DateFormatUtil.toDateTimeFormat(value);
        }
        return value;
    }

}
