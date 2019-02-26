package com.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReadConfig implements Serializable{

	private static final long serialVersionUID = 1L;
	Properties configFile;
	public static Map<String, String> parseArguments(String... args){
		Map<String, String> commandListArguments = new HashMap<String, String>();
		for (int i = 0; i < args.length; i++) {
			String[] argument = args[i].split("=");
			commandListArguments.put(argument[0].replaceAll("--", ""), argument[1]);
			
		}
		return commandListArguments;
	}


	public ReadConfig(String configfilepath) {
		configFile = new java.util.Properties();
		try {
			this.getClass().getClassLoader().getResourceAsStream(configfilepath);
			configFile.load(this.getClass().getClassLoader().getResourceAsStream(configfilepath));
		} catch (Exception eta) {
			eta.printStackTrace();
		}
	}

	public String getProperty(String key) {
		String value = this.configFile.getProperty(key);
		return value;
	}
	public void setProperty(String key,String value) {
		configFile.setProperty(key,value);
	}
}
