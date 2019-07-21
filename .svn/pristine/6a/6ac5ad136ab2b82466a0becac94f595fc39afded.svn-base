package com.fuhao.mysql;

import org.junit.Test;

import com.fuhao.data.access.FuHaoAccess;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class CRUDTest {
	
	@Test
	public void putTest() {
		JSONArray j = new JSONArray();
		JSONObject o = new JSONObject();
		java.sql.Date d = new java.sql.Date(System.currentTimeMillis());
		o.put("Friend_id", "6");
		o.put("Friend_name", "Ame");
		o.put("Friend_age", "26");
		o.put("Friend_date", d.toString());
		j.add(o);
		FuHaoAccess access =(FuHaoAccess) FuHaoAccess.getInstance();
		try {
			boolean b = access.put(j);
			System.out.println(b);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void getTest() {
		JSONArray j = new JSONArray();
		j.add("Friend_name");
		j.add("Friend_age");
		j.add("Friend_date");
		String condition = "where Friend_id <3";
		FuHaoAccess access =(FuHaoAccess) FuHaoAccess.getInstance();
		try {
			JSONArray jsonArray = access.get(j, condition);
			System.out.println(jsonArray);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void getTest2() {
		JSONArray j = new JSONArray();
		j.add("Gdate");
		j.add("Gtimestamp");
		j.add("Gdatetime");
		j.add("Gyear");
		String condition = "where Gtime =\" 08:11:09\"";
		FuHaoAccess access =(FuHaoAccess) FuHaoAccess.getInstance();
		try {
			JSONArray jsonArray = access.get(j, condition);
			System.out.println(jsonArray);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}	
