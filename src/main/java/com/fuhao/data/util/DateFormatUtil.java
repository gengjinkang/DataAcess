package com.fuhao.data.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFormatUtil {
	
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
	
	private static SimpleDateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
	
	private static SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	private static Timestamp t = new Timestamp(System.currentTimeMillis());
	
	public static Date toDate(String value) {
		 java.util.Date d = null; 
		 try { 
		  d = dateFormat.parse(value); 
		  Date date = new Date(d.getTime()); 
		  return date; 
		 } catch (Exception e) { 
		  e.printStackTrace(); 
		 }
		return null; 
	}
	
	public static Time toTime(String value) {
		 java.util.Date d = null; 
		 try { 
		  d = timeFormat.parse(value); 
		  Time time = new Time(d.getTime());
		  return time; 
		 } catch (Exception e) { 
		  e.printStackTrace(); 
		 }
		return null; 
	}
	
	public static Timestamp toTimestamp(String value) {
		try {  
            t = Timestamp.valueOf(value);   
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
		return t;
	}
	
	public static Timestamp toDateTimeFormat(String value) {
		java.util.Date d = null; 
		String datetime = null;
		try { 
			d = datetimeFormat.parse(value);
		    datetime = datetimeFormat.format(d);
			Timestamp t = toTimestamp(datetime);
			 return t; 
		} catch (Exception e) { 
		    e.printStackTrace(); 
		}
			return null; 
	}
	
}
