package com.parser;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;

public class JsonParserDynamicTable implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public Map<String, String> parseJSON(String jsonlog,String logtypepath,String tablename,String logtimepath,String logtimeformat,String logtimecolumn) {
		System.out.println(jsonlog);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Calendar cal = Calendar.getInstance();
		long offset = cal.getTimeZone().getOffset(System.currentTimeMillis());
		Date logtime = new Date(System.currentTimeMillis() - offset);
		Map<String, String> planeMap = new HashMap<String, String>();
		try {
			Map<String, Object> flattenedJsonMap =  new JsonFlattener(jsonlog).withFlattenMode(FlattenMode.KEEP_ARRAYS).flattenAsMap();
			System.out.println("Table : " + flattenedJsonMap.get(logtypepath)
					+ (flattenedJsonMap.get(logtypepath) != null));
			if(jsonlog.getBytes().length>=1048576) {
				planeMap = new HashMap<String, String>();
				planeMap.put("message", jsonlog.substring(0, 1000000)+"...");
				planeMap.put(tablename, "errorlog");
				planeMap.put("error_message", "Maximum allowed row size exceeded. Allowed: 1048576 Row size: " + jsonlog.getBytes().length+" bytes");
				planeMap.put(logtimecolumn,  dateFormat.format(logtime));
			}
			else if (flattenedJsonMap.get(logtypepath) != null) {
				for (String key : flattenedJsonMap.keySet()) {
					if(key.length()<=128)
					{
						String value = Objects.toString(flattenedJsonMap.get(key),"");
						planeMap.put(key.replace("_", "").replace(".", "_").replace(" ", "_").replaceAll("[^\\w_]+", "").trim().toLowerCase(), value);
					}
				}
				String tablenamestring="";
				tablenamestring=Objects.toString(flattenedJsonMap.get(logtypepath));
				
				planeMap.put(tablename, tablenamestring.replace("_", "").replace(".", "_").replace(" ", "_").replaceAll("[^\\w_]+", "").trim().toLowerCase());
				
				if (flattenedJsonMap.containsKey(logtimepath)
						&& flattenedJsonMap.get(logtimepath) != null
						&& (Objects.toString( flattenedJsonMap.get(logtimepath))).toLowerCase().trim() != "") {
					if (logtimeformat.equals("milliseconds")
							&& flattenedJsonMap.get(logtimepath) != null) {
						logtime = new Date(Long.parseLong((Objects.toString( flattenedJsonMap.get(logtimepath))).toLowerCase().trim()));
					} else {

						SimpleDateFormat formatter = new SimpleDateFormat(logtimeformat);
						formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
						try {
							logtime = formatter.parse((Objects.toString( flattenedJsonMap.get(logtimepath))).toLowerCase().trim());
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				System.out.println("Logtime:"+dateFormat.format(logtime));
				planeMap.put(logtimecolumn,  dateFormat.format(logtime));
				
				Calendar minpartitiondate = Calendar.getInstance();    
				minpartitiondate.add(Calendar.DATE, -30);
				Calendar maxpartitiondate = Calendar.getInstance();    
				maxpartitiondate.add(Calendar.DATE, 15);
				if(logtime.before(minpartitiondate.getTime()) || logtime.after(maxpartitiondate.getTime()) )
				{
					planeMap = new HashMap<String, String>();
					planeMap.put("message", jsonlog);
					planeMap.put(tablename, "errorlog");
					planeMap.put("error_message","Logtime is out of bound 31days-16days of partition : "+dateFormat.format(logtime));
					logtime = new Date(System.currentTimeMillis() - offset);
					planeMap.put(logtimecolumn,  dateFormat.format(logtime));
				}
			} else {
				planeMap = new HashMap<String, String>();
				planeMap.put("message", jsonlog);
				planeMap.put(tablename, "errorlog");
				planeMap.put("error_message", "No key column found " + logtypepath);
				planeMap.put(logtimecolumn,  dateFormat.format(logtime));
			}
		} catch (Exception ex) {
			System.out.println("ErrorText : "+ex.toString());
			planeMap = new HashMap<String, String>();
			planeMap.put("message", jsonlog);
			planeMap.put(tablename, "errorlog");
			planeMap.put("error_message", ex.toString());
			planeMap.put(logtimecolumn,  dateFormat.format(logtime));
		}
		return planeMap;
	}
}
