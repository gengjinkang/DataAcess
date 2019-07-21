package com.fuhao.data;

import java.util.Date;

import net.sf.json.JSONObject;

public class JSONObjectTest {
	public static void main(String[] args) {
		JSONObject json=new JSONObject();
		Date date=new Date(System.currentTimeMillis());
		System.out.println(date);
		json.put("hello", date);
		System.out.println(json);
		System.out.println(json.get("hello"));
	}
}
