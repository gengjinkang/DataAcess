package com.fuhao.data.datasource.api;

import org.junit.Test;

public class ThreadLocalTest {
	private ThreadLocal<String>str=new ThreadLocal<String>();
	
	
	@Test
	public void test(){
		str.set(null);
		System.out.println(str.get());
	}
	public static boolean testA() {
		boolean a=true;
		try {
			int x=10/0;
			
		} catch (Exception e) {
			a=false;
		}finally{
			return a;
		}
		
	}
	public static void main(String[] args) {
		try {
			System.out.println(testA());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
