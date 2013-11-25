package hu.bme.bigdata.mapreduce.jobs;

import hu.bme.bigdata.mapreduce.mappers.HostDownloadMapper;
import hu.bme.bigdata.mapreduce.reducers.HostDownloadReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HostDownloadJob {

	public static void main(String[] args) throws Exception {
		
		// Create a new Job
	     Configuration conf = new Configuration();
	     Job job = Job.getInstance(conf);
	     job.setJarByClass(HostDownloadJob.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);
	     // Specify various job-specific parameters     
	     job.setJobName("hostbydownload");
	     job.setMapperClass(HostDownloadMapper.class);
	     job.setReducerClass(HostDownloadReducer.class);
	     FileInputFormat.setInputPaths(job, new Path(args[0]));
	     Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
	     // Submit the job, then poll for progress until the job is complete
	     job.waitForCompletion(true);
	     
	}

}
