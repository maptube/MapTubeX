package WordCount020;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.conf.*;
//import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCountDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception
	{
		JobConf conf = new JobConf(WordCountDriver.class);
		conf.setJobName("WordCount");
		
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		//conf.setInputPath(new Path("src"));
		//conf.setOutputPath(new Path("out"));
		
		//specify a mapper
		conf.setMapperClass(WordCountMapper.class);
		
		//specify a reducer
		conf.setReducerClass(WordCountReducer.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient client = new JobClient();
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
		System.exit(res);
	}
	
}
