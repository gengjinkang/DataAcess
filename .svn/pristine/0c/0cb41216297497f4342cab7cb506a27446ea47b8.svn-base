package com.fuhao.data.column;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class ColumnSort {
	private static Map<String, String> columnMap = new HashMap<String, String>();
	private static  Map<String, String> tableMap = new HashMap<String, String>();
	static {
		columnMap.put("name", "user");
		columnMap.put("age", "user");
		columnMap.put("address", "info");
		columnMap.put("desc", "info");
		columnMap.put("colleague","emp");
	}
	
	public static void main(String[] args) {
		JSONObject data = new JSONObject();
		data.put("_key", "1001");
		data.put("name", "xxd");
		data.put("address", "中国山西");
		data.put("age", "21");
		data.put("desc", "做一个有内涵的人");
		JSONArray colleague=new JSONArray();
		colleague.add("郭健");
		colleague.add("庚金康");
		colleague.add("陈书奇");
		data.put("colleague", colleague);
		System.out.println(data);
		sort(data);
	}
	private static void sort(JSONObject data) {
		JSONObject datas=new JSONObject();
		if(!data.containsKey("_key"))return ;
		Object key=data.get("_key");
		Set<String> keySet = data.keySet();
		for (String column : keySet) {
			if(!"_key".equals(column)){
				String tName=columnMap.get(column);
				JSONObject row=(JSONObject) datas.get(tName);
				if(row==null){
					row=new JSONObject();
					row.put("_key", key);
					datas.put(tName, row);
				}
				row.put(column, data.get(column));
			}
		}
		System.out.println(datas);
	}
}
