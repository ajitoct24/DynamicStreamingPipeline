package com.pipeline;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import com.util.ReadConfig;

public class DynamicPipelineWrapper {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String, String> arguments = ReadConfig.parseArguments(args);
		if (!arguments.containsKey("config")) {
			try {
				throw new Exception("Invalid arguments");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		final ReadConfig config = new ReadConfig(arguments.get("config"));
		if(config.getProperty("jobtype").equals("Batch"))
		{
			if (arguments.containsKey("reportdate")) {
				config.setProperty("reportdate", arguments.get("reportdate"));
			} else {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Calendar c = Calendar.getInstance();
				c.add(Calendar.DATE, -1);
				config.setProperty("reportdate", sdf.format(c.getTime()));
			}
			try {
				DataflowBatchPipeline.run(config);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else
		{
			try {
				DataflowStreamingPipeline.run(config);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
