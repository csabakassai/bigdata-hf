package hu.bme.bigdata.mapreduce.jobs;

import hu.bme.bigdata.mapreduce.mappers.DownloadCountMapper;
import hu.bme.bigdata.mapreduce.mappers.HostDownloadMapper;
import hu.bme.bigdata.mapreduce.reducers.DownloadCountReducer;
import hu.bme.bigdata.mapreduce.reducers.HostDownloadReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HostDownloadJob {

	public static void main(String[] args) throws Exception {
		
		// Create a new Job
	     Job job = Job.getInstance();
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
	     Path tempPath = new Path(args[1]);
	     FileOutputFormat.setOutputPath(job, tempPath);
	     // Submit the job, then poll for progress until the job is complete
	     job.waitForCompletion(true);
	     
	     
	     Job finalJob = Job.getInstance();
	     finalJob.setJarByClass(HostDownloadJob.class);
	     finalJob.setMapOutputKeyClass(IntWritable.class);
	     finalJob.setMapOutputValueClass(Text.class);
	     finalJob.setOutputKeyClass(IntWritable.class);
	     finalJob.setOutputValueClass(IntWritable.class);
	     // Specify various job-specific parameters     
	     finalJob.setJobName("finalhostbydownload");
	     finalJob.setMapperClass(DownloadCountMapper.class);
	     finalJob.setReducerClass(DownloadCountReducer.class);
	     FileInputFormat.setInputPaths(finalJob, tempPath);
	     Path outputDir = new Path(args[2]);
	     FileOutputFormat.setOutputPath(finalJob, outputDir);
	     // Submit the job, then poll for progress until the job is complete
	     finalJob.waitForCompletion(true);
	     
	     
	     
	     
	}

}
