package hu.bme.bigdata.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DownloadCountReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
		
		
		@Override
		public void reduce(IntWritable inputKey, Iterable<Text> inputValues, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text intWritable : inputValues) {
				sum = sum + 1;
			}
			context.write(inputKey, new IntWritable(sum));
		}

}
