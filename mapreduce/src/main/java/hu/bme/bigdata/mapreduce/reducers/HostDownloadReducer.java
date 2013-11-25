package hu.bme.bigdata.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HostDownloadReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	@Override
	public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable intWritable : inputValues) {
			sum = sum + Integer.parseInt(intWritable.toString());
		}
		context.write(inputKey, new IntWritable(sum));
	}
	
	
	

}
