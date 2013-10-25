package maptubex.functions;

//TotalOrderSort is going to have to stick with the older mapred API as TotalOrderPartitioner doesn't exist in 0.20.2.
//The sort example in the distribution source does this anyway.

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner; //location in >0.20.2
//import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalOrderSort {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		Path partitionFile = new Path(args[1] + "_partitions.lst");
		Path outputStage = new Path(args[1] + "_staging");
		Path outputOrder = new Path(args[1]);
		// Configure job to prepare for sampling
		Job sampleJob = new Job(conf, "TotalOrderSortStage");
		sampleJob.setJarByClass(TotalOrderSort.class);
		// Use the mapper implementation with zero reduce tasks
//		sampleJob.setMapperClass(LastAccessDateMapper.class);
		sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(sampleJob, inputPath);
		// Set the output format to a sequence file
		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);
		// Submit the job and get completion code.
		int code = sampleJob.waitForCompletion(true) ? 0 : 1;
		
		if (code == 0) {
			Job orderJob = new Job(conf, "TotalOrderSortStage");
			orderJob.setJarByClass(TotalOrderSort.class);
			// Here, use the identity mapper to output the key/value pairs in
			// the SequenceFile
			orderJob.setMapperClass(Mapper.class);
//			orderJob.setReducerClass(ValueReducer.class);
			// Set the number of reduce tasks to an appropriate number for the
			// amount of data being sorted
			orderJob.setNumReduceTasks(10);
			// Use Hadoop's TotalOrderPartitioner class
			orderJob.setPartitionerClass(TotalOrderPartitioner.class);
			// Set the partition file
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);
			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(Text.class);
			// Set the input to the previous job's output
			orderJob.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
			// Set the output path to the command line parameter
			TextOutputFormat.setOutputPath(orderJob, outputOrder);
			// Set the separator to an empty string
			orderJob.getConfiguration().set("mapred.textoutputformat.separator", "");
			// Use the InputSampler to go through the output of the previous
			// job, sample it, and create the partition file
//			InputSampler.writePartitionFile(orderJob,new InputSampler.RandomSampler(.001, 10000));
			// Submit the job
			code = orderJob.waitForCompletion(true) ? 0 : 2;
		}
		// Clean up the partition file and the staging directory
		FileSystem.get(new Configuration()).delete(partitionFile, false);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		System.exit(code);
	}
	
	/*public static class LastAccessDateMapper extends
		Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			outkey.set(parsed.get("LastAccessDate"));
			context.write(outkey, value);
		}
	}*/
	
	/*public static class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}*/

}
