package com.fuhao.data.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

public class Configurations {
	public static final Properties PROPERTIES = new Properties();

	static {
		try {
			PROPERTIES.load(Configurations.class.getClassLoader()
					.getResourceAsStream("data.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据属性名查找属性值
	 * 
	 * @param key属性名称
	 * @return 返回属性值
	 */
	public static Object getProperty(String key) {
		return PROPERTIES.get(key);
	}

	/**
	 * 加載properties文件
	 * 
	 * @param inStream
	 * @throws IOException
	 */
	public synchronized static void load(InputStream inStream)
			throws IOException {
		PROPERTIES.load(inStream);
	}

	/**
	 * 加载properties文件
	 * 
	 * @param reader
	 * @throws IOException
	 */
	public synchronized static void load(Reader reader) throws IOException {
		PROPERTIES.load(reader);
	}

	/**
	 * 加载xml文件
	 * 
	 * @param in
	 * @throws IOException
	 * @throws InvalidPropertiesFormatException
	 */
	public synchronized static void loadFromXML(InputStream in)
			throws IOException, InvalidPropertiesFormatException {
		PROPERTIES.loadFromXML(in);
	}
}
