package hu.bme.bigdata.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadCountMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private static Logger logger = LoggerFactory.getLogger(HostDownloadMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] splitted = value.toString().split("	");
		String hostname = splitted[0];
		int count = Integer.parseInt( splitted[1] );
		
		context.write(new IntWritable(count), new Text(hostname));

	}

}