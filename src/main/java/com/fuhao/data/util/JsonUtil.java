package com.fuhao.data.util;


import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JsonUtil {

    public static JsonBuilder builder(){
        return new JsonBuilder();
    }

    public static class JsonBuilder{

        private JSONObject jsonObject = new JSONObject();

        public JsonBuilder put(String key,Object value){
            this.jsonObject.put(key,value);
            return this;
        }
        public JSONObject build(){
            return jsonObject;
        }
    }

   public static JSONArray builderArray(Object... objects){
        JSONArray array = new JSONArray();
        for(int i=0;i<objects.length;i++){
            array.add(objects[i]);
        }
        return array;
   }
}
