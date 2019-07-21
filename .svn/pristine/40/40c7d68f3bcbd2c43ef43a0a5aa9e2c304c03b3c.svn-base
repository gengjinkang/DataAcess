package com.fuhao.data;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.junit.Test;

import com.fuhao.data.access.FuHaoAccess;
import com.fuhao.data.access.FuHaoData;

public class FuhaoAccessTes {
	@Test
	public void testAccessTes() {
	 
	FuHaoData fd = FuHaoAccess.getInstance();
		JSONObject obj = new JSONObject();
		obj.put("_key", "10086");
		obj.put("account", "tome");
		obj.put("passwd", "tome123");
		JSONArray colleague = new JSONArray();
		colleague.add("郭健");
		colleague.add("庚金康");
		colleague.add("陈书奇");
		obj.put("colleague", colleague);
		JSONArray datas=new JSONArray();
		datas.add(obj);
		System.out.println(datas);
		try {
			fd.put(datas);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
