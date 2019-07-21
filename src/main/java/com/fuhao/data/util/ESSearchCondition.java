package com.fuhao.data.util;

public class ESSearchCondition {
    /**
     * 没分析出来前的全文检索
     * @param key    字段
     * @param value  值
     * @return
     */
    public static String full(String key,String value) {
        return like(key, "*"+value+"*");
    }
    
    public static String term(String key ,String value) {
        //将key和对应value拼接入精确查询term语句
        StringBuffer term = new StringBuffer(); 
            term.append("{'term':{'")
            .append(key)
            .append("':'")
            .append(value)
            .append("'}}");
        //返回term精确查询语句  
        return term.toString();
    }
    
    public static String notEqual(String key, String value) {
        //将key和value拼接入不等于must_not+term查询语句
        StringBuffer stf = new StringBuffer("{'bool':{'must_not':[");
        stf.append("{'term':{'")
        .append(key)
        .append("':'")
        .append(value)
        .append("'}}");
        stf.append("]}}");
        //返回must_not不等于查询语句
        return stf.toString();
    }
    
    
    public static String rather(String str, String gt) {
        //将key和value拼接入大于range+gt查询语句
        StringBuffer range = new StringBuffer();
        range.append("{'range':{'")
             .append(str)
             .append("':{'gt':")
             .append(gt)
             .append("}}}");
        //返回拼接好的ranger查询语句
        return range.toString();
    }

    public static String ratherEqual(String str, String gte) {
        //将key和value拼接入大于等于range+gte查询语句
        StringBuffer range = new StringBuffer();
        range.append("{'range':{'")
             .append(str)
             .append("':{'gte':")
             .append(gte)
             .append("}}}");
        //返回拼接好的range查询语句
        return range.toString();
    }

    public static String less(String str, String lt) {
        //将key和value拼接入小于range+lt查询语句
        StringBuffer range = new StringBuffer();
        range.append("{'range':{'")
             .append(str)
             .append("':{'lt':")
             .append(lt)
             .append("}}}");
        //返回拼接好的range查询语句
        return range.toString();
    }

    public static String lessEqual(String str, String lte) {
        //将key和value拼接入小于等于range+lte查询语句
        StringBuffer range = new StringBuffer();
        range.append("{'range':{'")
             .append(str)
             .append("':{'lte':")
             .append(lte)
             .append("}}}");
        //返回拼接好的range查询语句
        return range.toString();
    }
    
    public static String range(String str,String min,String max) {
        //将key和value拼接入范围,range+gt+lt查询语句,相似于between,and
        StringBuffer range = new StringBuffer();
        range.append("{'range':{'")
             .append(str)
             .append("':{'gt':")
             .append(min)
             .append(",'lt':")
             .append(max)
             .append("}}}");
        //返回拼接好的range范围查询语句 
        return range.toString();
    }
    
    /**
     * 拼接like语句对象
     * @param key    字段
     * @param regexp 值得正则表达式
     * @return
     */
    public static String like(String key,String regexp) {
        //将key和value拼接入模糊查询(正则表达式)regexp语句
        StringBuffer like = new StringBuffer();
        regexp = regexp.substring(1,regexp.length()-1);
        like.append("{'regexp': { '"+key+"': '"+regexp+"'}}");
        //返回拼接好的模糊查询语句
        return like.toString();
    }
    
    /**
     * 拼接must关系
     * @param sarr 数组内元素
     */
    public static String mustSpell(String[] sarr) {
        //给已经拼好的各种条件关系加上must(sql里的and)且关系
        StringBuffer stf = new StringBuffer("{'bool':{'must':[");
        //循环遍历数组里的各类条件语句,拼接好    
        for (int i = 0; i < sarr.length; i++) {
            if (i<sarr.length-1) {
                stf.append(sarr[i]+",");
            }else {
                stf.append(sarr[i]);
            }
        }
        stf.append("]}}");
        //返回拼接好的must且关系语句
        return stf.toString();
    }
    
    /**
     * 拼接should关系
     * @param sarr 数组内元素
     */
    public static String shouldSpell(String[] sarr) {
        //将已经拼好的各种条件关系加上should(sql里的or)或关系
        StringBuffer stf = new StringBuffer("{'bool':{'should':[");
        //循环遍历数组里的各类条件语句,拼接好
        for (int i = 0; i < sarr.length; i++) {
            if (i<sarr.length-1) {
                stf.append(sarr[i]+",");
            }else {
                stf.append(sarr[i]);
            }
        }
        stf.append("]}}");
        //返回拼接好的should或关系语句
        return stf.toString();
    }
    
    /**
     * 拼接mustnot语句
     * @param sarr 
     */
    public static String mustNotSpell(String[] sarr) {
        //将已经拼好的各类条件关系加上must_not(!)否的关系
        StringBuffer stf = new StringBuffer("{'bool':{'must_not':[");
        //循环遍历数组里的各类条件语句,拼接好
        for (int i = 0; i < sarr.length; i++) {
            if (i<sarr.length-1) {
                stf.append(sarr[i]+",");
            }else {
                stf.append(sarr[i]);
            }
        }
        stf.append("]}}");
        //返回拼接好的must_not否关系语句
        return stf.toString();
    }
    
    public static String mustSpell(String str1, String str2) {
        //重载拼接and关系,将两个元素加入数组里
        String[] str = new String[2];
        str[0]=str1;
        str[1]=str2;
        //调用mustspell(arr[])
        return mustSpell(str);
    }

    public static String shouldSpell(String str1, String str2) {
        //重载拼接should关系,将两个元素加入数组里
        StringBuffer stf = new StringBuffer("{'bool':{'should':[");
        stf.append(str1+",").append(str2);
        stf.append("]}}");
        //调用shouldspell(arr[])
        return stf.toString();
    }
}
