package com.pipeline;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import com.bigquery.BQTables;
import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;
import com.parser.JsonParserDynamicTable;
import com.parser.JsonParserStaticTable;
import com.util.ReadConfig;

public class DataflowBatchPipeline {

	public static void run(ReadConfig config) throws Exception {
		
		
		System.out.println("Running for " + config.getProperty("reportdate"));
		final BQTables bqtable = new BQTables();
		final JsonParserDynamicTable jsonparserdynamictable = new JsonParserDynamicTable();
		final JsonParserStaticTable jsonparserstatictable = new JsonParserStaticTable();
		bqtable.initializeTablelist();
		// Start by defining the options for the pipeline.

		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		if (config.getProperty("runner").equals("DataflowRunner")) {
			pipelineOptions.setRunner(DataflowRunner.class);
		} else {
			pipelineOptions.setRunner(DirectRunner.class);
		}

		pipelineOptions.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
		int maxNumWorkers = Integer.parseInt(config.getProperty("maxnumworkers"));
		pipelineOptions.setMaxNumWorkers(maxNumWorkers);
		pipelineOptions.setProject(config.getProperty("projectid"));
		pipelineOptions.setJobName(("gcs-" + config.getProperty("bucketname") + "-"
				+ (config.getProperty("gcsfileprefix").replaceAll("v_reportdate", config.getProperty("reportdate")))
				+ "-bq-" + config.getProperty("datasetid")).replace("_", "").replace("/", "0"));

		pipelineOptions.setStreaming(false);
		pipelineOptions.setTempLocation(config.getProperty("temp"));
		pipelineOptions.setStagingLocation(config.getProperty("staging"));

		Pipeline p = Pipeline.create(pipelineOptions);

		Storage storage = StorageOptions.getDefaultInstance().getService();
		Page<Blob> blobs = storage.list(config.getProperty("bucketname"), BlobListOption.currentDirectory(),
				BlobListOption.prefix(config.getProperty("gcsfileprefix").replaceAll("v_reportdate",
						config.getProperty("reportdate"))));
		ArrayList<PCollection<String>> pcollectionlist = new ArrayList<>();
		storage.get(config.getProperty("bucketname"));
		System.out.println(
				config.getProperty("gcsfileprefix").replaceAll("v_reportdate", config.getProperty("reportdate")));
		if (blobs.getValues().iterator().hasNext()) {
			for (Blob blob : blobs.iterateAll()) {
				System.out.println("Reading from " + blob.getName());
				PCollection<String> eachfileread = p
						.apply(TextIO.read().from("gs://" + config.getProperty("bucketname") + "/" + blob.getName()));
				pcollectionlist.add(eachfileread);

			}
			PCollection<String> inLogsFromPubSub = PCollectionList.of(pcollectionlist).apply(Flatten.pCollections());

			PCollection<Map<String, String>> printedpcoll = inLogsFromPubSub
					.apply(ParDo.of(new DoFn<String, Map<String, String>>() {
						private static final long serialVersionUID = 1L;

						@ProcessElement
						public void processElement(DoFn<String, Map<String, String>>.ProcessContext c)
								throws Exception {
							String message = c.element();
							Map<String, String> jsonmap = null;
							if (config.getProperty("jsonparser").equals("JsonParserDynamicTable")) {
								jsonmap = jsonparserdynamictable.parseJSON(message.toString(),
										config.getProperty("lognamepath"), config.getProperty("tablenamecolunm"),
										config.getProperty("logtimepath"), config.getProperty("logtimeformat"),
										config.getProperty("logtimecolumn"));
							} else if (config.getProperty("jsonparser").equals("JsonParserStaticTable")) {
								jsonmap = jsonparserstatictable.parseJSON(message.toString(),
										config.getProperty("targettablename"), config.getProperty("tablenamecolunm"),
										config.getProperty("logtimepath"), config.getProperty("logtimeformat"),
										config.getProperty("logtimecolumn"));
							}
							System.out.println(jsonmap.get(config.getProperty("tablenamecolunm")) + jsonmap.keySet());
							if (!Objects.toString(jsonmap.get(config.getProperty("tablenamecolunm")))
									.equals("apilog_null")) {
								try {
									jsonmap = bqtable.getTable(config.getProperty("datasetid"),
											jsonmap.get(config.getProperty("tablenamecolunm")), jsonmap);
								} catch (Exception ex) {

								}

								c.output(jsonmap);
							}

						}
					}));
			printedpcoll.apply(
					BigQueryIO.<Map<String, String>>write().withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
							.withWriteDisposition(WriteDisposition.WRITE_APPEND)
							.to(new DynamicDestinations<Map<String, String>, String>() {
								private static final long serialVersionUID = 1L;

								public String getDestination(ValueInSingleWindow<Map<String, String>> jsonmap) {
									System.out.println(
											"Tablex : " + jsonmap.getValue().get(config.getProperty("tablenamecolunm"))
													.toString().trim().toLowerCase() + jsonmap);
									DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
									DateFormat datepartitionFormat = new SimpleDateFormat("yyyyMMdd");
									String partitionid = "";
									try {
										Date logtime = dateFormat.parse(jsonmap.getValue()
												.get(config.getProperty("logtimecolumn")).toString().trim());
										partitionid = datepartitionFormat.format(logtime);
									} catch (ParseException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}

									return jsonmap.getValue()
											.getOrDefault(config.getProperty("tablenamecolunm"), "errorlog").toString()
											.trim().toLowerCase() + "$" + partitionid;
								}

								@Override
								public TableDestination getTable(String event) {
									System.out.println("table for " + event + " - " + config.getProperty("projectid")
											+ ":" + config.getProperty("datasetid") + "." + event.trim().toLowerCase());
									return new TableDestination(config.getProperty("projectid") + ":"
											+ config.getProperty("datasetid") + "." + event.trim().toLowerCase(),
											"table for " + event);
								}

								@Override
								public TableSchema getSchema(String event) {
									TableSchema schema = new TableSchema();
									List<TableFieldSchema> fieldlist = new ArrayList<TableFieldSchema>();
									Map<String, String> fieldnamemap = bqtable.getSchema(
											config.getProperty("datasetid"), event.trim().toLowerCase().split("$")[0]);
									fieldnamemap = bqtable.fetchSchema(config.getProperty("datasetid"),
											event.split("$")[0]);
									for (String fieldname : fieldnamemap.keySet()) {
										fieldlist.add(new TableFieldSchema().setName(fieldname)
												.setType(fieldnamemap.get(fieldname.trim().toLowerCase())));
									}
									schema.setFields(fieldlist);
									System.out
											.println("Schema for" + event.split("$")[0]
													+ bqtable.getSchema(config.getProperty("datasetid"),
															event.split("$")[0].trim().toLowerCase())
													+ schema.getFields());

									return schema;
								}
							}).withFormatFunction(new SerializableFunction<Map<String, String>, TableRow>() {
								private static final long serialVersionUID = 1L;

								public TableRow apply(Map<String, String> jsonmap) {
									TableRow row = new TableRow();
									row.putAll(jsonmap);
									return row;
								}
							}));
			p.run().waitUntilFinish();

			for (Blob blob : blobs.iterateAll()) {
				System.out.println("Moving from " + blob.getName() + " to " + config.getProperty("archivedfolder") + "/"
						+ blob.getName());
				CopyWriter copyWriter = blob.copyTo(config.getProperty("bucketname"),
						config.getProperty("archivedfolder") + "/" + blob.getName());
				copyWriter.getResult();
				blob.delete();
			}
		} else {
			System.out.println("Skipping dataflow as 0 files found at GCS");
		}

	}
}
