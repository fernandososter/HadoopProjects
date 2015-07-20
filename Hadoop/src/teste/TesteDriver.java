package teste;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TesteDriver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
	
		Job job = new Job(getConf()); 
		job.setJarByClass(TesteDriver.class);
		job.setJobName("TesteDriver");
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:54310/DETRAF/input/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/DETRAF/output/output"));
		
		job.setMapperClass(TesteMapper.class);
		job.setReducerClass(TesteReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return ( job.waitForCompletion(true) ? 0 : 1);
		
	}

	
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new TesteDriver(),args); 
		System.exit(exitCode);
		
	}
	
	
	
}
