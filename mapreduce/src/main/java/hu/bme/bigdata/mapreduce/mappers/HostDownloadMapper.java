package hu.bme.bigdata.mapreduce.mappers;

import hu.bme.bigdata.mapreduce.util.NasaLogLine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostDownloadMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static Logger logger = LoggerFactory.getLogger(HostDownloadMapper.class);
	
	public HostDownloadMapper() {
		logger.info("Create mapper");
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			NasaLogLine nasaLogLine = new NasaLogLine(value);
			String hostName = nasaLogLine.getHost();
			int i = 0;
			if (nasaLogLine.getRequest() != null
					&& nasaLogLine.getRequest().endsWith("html")) {
				i = 1;
				
			}
			context.write(new Text(hostName), new IntWritable(i));
			
		} catch (Exception e) {
			logger.error("Failed to parse line: " + value.toString());
			throw new RuntimeException("|" + value.toString() + "|", e);
		}

	}

}
