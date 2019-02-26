package com.bigquery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

public class BQTables implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final int bqRetryCount = 30;
	private Map<String, Map<String, String>> tablelist;

	public Map<String, String> getTable(String datasetid, String eventname, Map<String, String> eventrowmap) {
		if (tablelist == null) {
			initializeTablelist();
		}
		System.out.println("Eventname : " + eventname);

		if (tablelist.containsKey(eventname.trim().toLowerCase())) {
			Map<String, String> tableschema = getSchema(datasetid, eventname);

			if (!matchSchema(eventrowmap, tableschema)) {
				Map<String, String> newschema = null;
				int count = bqRetryCount;
				while (count > 0) {
					try {
						System.out.println("Updateing : Schema not match");
						newschema = updateTable(datasetid, eventname, eventrowmap);
						System.out.println("Table update successful");
						count = 0;
					} catch (Exception ex1) {
						if (count == 0) {
							System.out.println("Failed to update table "+eventname);
							newschema = fetchSchema(datasetid, eventname.trim().toLowerCase());
						} else {
							count--;
							System.out.println("Update retry : " + eventname + ":" + count);
						}
					}
				}
				tablelist.put(eventname.trim().toLowerCase(), newschema);
			} else {
				System.out.println("Not Updating : Schema match");
			}
		} else {
			// check if table present
			Map<String, String> newschema = createTable(datasetid, eventname, eventrowmap);
			tablelist.put(eventname.trim().toLowerCase(), newschema);
		}
		Map<String, String> newtableschema =getSchema(datasetid, eventname);
		if (!matchSchema(eventrowmap, newtableschema))
		{
			System.out.println("Remove fields not in table "+eventname);
			newtableschema =fetchSchema(datasetid, eventname);
			tablelist.put(eventname.trim().toLowerCase(), newtableschema);
			Map<String, String> neweventrowmap= new HashMap<String, String>();
			for (String column : eventrowmap.keySet()) {
				if (newtableschema.containsKey(column)) {
					neweventrowmap.put(column,eventrowmap.get(column));
				}
			}
			eventrowmap= neweventrowmap;
		}
		for (String column : newtableschema.keySet()) {
			if (!eventrowmap.containsKey(column)) {
				eventrowmap.put(column,"");
			}
		}
		return eventrowmap;
	}

	public Boolean matchSchema(Map<String, String> eventschema, Map<String, String> tableschema) {
		try {
			System.out.println("Check schema match");
			for (String column : eventschema.keySet()) {
				if (!tableschema.containsKey(column.trim().toLowerCase())) {
					System.out.println("Check schema match : False");
					System.out.println("Schema match:" + tableschema + ":" + column);
					return false;
				}
			}
			System.out.println("Check schema match : True");
			return true;
		} catch (Exception ex) {
			System.out.println("Exeception in Schema match :" + ex);
			return false;
		}
	}

	public Map<String, String> updateTable(String datasetId, String eventtablename,
			Map<String, String> eventtablerowmap) {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		Table oldTable = bigquery.getTable(datasetId, eventtablename.trim().toLowerCase());
		// Table field definition
		List<Field> fieldList = new ArrayList<Field>();
		Map<String, String> oldschema = fetchSchema(datasetId, eventtablename.trim().toLowerCase());
		for (String fieldname : oldschema.keySet()) {
			Field field = Field.of(fieldname, LegacySQLTypeName.valueOf(oldschema.get(fieldname.trim().toLowerCase())));
			fieldList.add(field);
		}
		for (String fieldname : eventtablerowmap.keySet()) {
			if (!oldschema.containsKey(fieldname.trim().toLowerCase())) {
				Field field = null;
				if (fieldname.equals("logtime")) {

					field = Field.of(fieldname, LegacySQLTypeName.TIMESTAMP);
				} else {
					field = Field.of(fieldname, LegacySQLTypeName.STRING);
				}
				fieldList.add(field);
			}
		}

		// Create a table
		Schema schema = Schema.of(fieldList);
		StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
				.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).setSchema(schema).build();

		bigquery.update(oldTable.toBuilder().setDefinition(tableDefinition).build());
		System.out.println("Table " + eventtablename + " updated" + schema.getFields());
		return fetchSchema(datasetId, eventtablename);
	}

	public Map<String, String> createTable(String datasetId, String eventtablename,
			Map<String, String> eventtablerowmap) {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		TableId tableId = TableId.of(datasetId, eventtablename);
		System.out.println("Creating" + eventtablename);
		if (bigquery.getTable(tableId) == null) {
			// Table field definition
			List<Field> fieldList = new ArrayList<Field>();

			for (String fieldname : eventtablerowmap.keySet()) {

				Field field = null;
				if (fieldname.equals("logtime")) {

					field = Field.of(fieldname, LegacySQLTypeName.TIMESTAMP);
				} else {
					field = Field.of(fieldname, LegacySQLTypeName.STRING);
				}
				fieldList.add(field);
			}

			// Create a table
			Schema schema = Schema.of(fieldList);
			StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
					.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY)).setSchema(schema).build();
			int createRetryCount = bqRetryCount;
			while (createRetryCount > 0) {
				try {
					Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition));
					FieldList newfieldList = createdTable.getDefinition().getSchema().getFields();
					Map<String, String> newshema = new HashMap<String, String>();
					for (Field field : newfieldList) {
						newshema.put(field.getName().trim().toLowerCase(), field.getType().name());
					}
					System.out.println("Table " + eventtablename + " created.");
					createRetryCount = 0;
					return newshema;
				} catch (Exception ex) {
					if (createRetryCount == 0) {
						System.out.println("Create table Exception(Trying update:" + ex);
						int updateRetryCount = bqRetryCount;
						while (updateRetryCount > 0) {
							try {
								Map<String, String> updatedschema = updateTable(datasetId, eventtablename,
										eventtablerowmap);
								updateRetryCount = 0;
								return updatedschema;

							} catch (Exception ex1) {
								if (updateRetryCount == 0) {
									System.out.println(eventtablename+" : Update retry after create fail "+(bqRetryCount-updateRetryCount)+":"+ex1.toString());
									return fetchSchema(datasetId, eventtablename.trim().toLowerCase());
								} else {
									updateRetryCount--;
								}
							}
						}
					} else {
						System.out.println(eventtablename+" : Create retry "+(bqRetryCount-createRetryCount)+":"+ex.toString());
						createRetryCount--;
					}
				}
			}
		} else {
			int updateRetryCount = bqRetryCount;
			while (updateRetryCount > 0) {
				try {
					System.out.println("Updateing : bigquery.getTable(tableId) is not null" + bigquery.getTable(tableId));
					Map<String, String> updatedschema = updateTable(datasetId, eventtablename, eventtablerowmap);
					updateRetryCount = 0;
					return updatedschema;
				} catch (Exception ex1) {
					if (updateRetryCount == 0) {
						return fetchSchema(datasetId, eventtablename.trim().toLowerCase());
					} else {
						System.out.println(eventtablename+" : Update retry "+(bqRetryCount-updateRetryCount)+":"+ex1.toString());
						updateRetryCount--;
					}
				}
			}
		}
		return fetchSchema(datasetId, eventtablename.trim().toLowerCase());
	}

	public Map<String, String> fetchSchema(String datasetId, String tablename) {
		System.out.println("Fetch schema : "+datasetId+" : "+tablename);
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		TableId tableId = TableId.of(datasetId, tablename);
		Table table = bigquery.getTable(tableId);
		table.reload();
		FieldList newfieldList = table.getDefinition().getSchema().getFields();
		Map<String, String> newshema = new HashMap<String, String>();
		for (Field field : newfieldList) {
			newshema.put(field.getName().trim().toLowerCase(), field.getType().name());
		}
		return newshema;
	}

	public Map<String, String> getSchema(String datasetId, String table) {
		if (!tablelist.containsKey(table.trim().toLowerCase())) {
			tablelist.put(table.trim().toLowerCase(), fetchSchema(datasetId, table));
		}
		return tablelist.get(table.trim().toLowerCase());
	}

	public void initializeTablelist() {
		Map<String, Map<String, String>> tablelist = new HashMap<String, Map<String, String>>();
		this.tablelist = tablelist;
	}
}
