package customPartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import customWritable.CustomDetrafReg;
import customWritable.CustomWritableDriver;
public class CustomPartitionerDriver extends Configured implements Tool{

	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Configuration(), new CustomPartitionerDriver(),args);
		System.exit(exit);
	}




	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(getConf()) ;
		
		job.setJarByClass(CustomWritableDriver.class);
		job.setJobName("CustomWritableDriver");
		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:54310/DETRAF/input/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/DETRAF/output/output"));
		
		job.setMapperClass( CustomPartitionerMapper.class);
		job.setReducerClass( CustomPartitionerReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(CustomDetrafReg.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setPartitionerClass(CustomDetrafPartitioner.class);
		
		return (job.waitForCompletion(true) ? 0 : 1); 
	}
	
	
}


 class CustomPartitionerMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		String[] tokens = value.toString().split(";"); 
		LongWritable l = new LongWritable(Long.parseLong(tokens[0])); 
		CustomDetrafReg reg = new CustomDetrafReg(); 
		reg.parser(tokens);
		context.write(l, reg);
	
	}
	
}
 
  class CustomPartitionerReducer extends Reducer<LongWritable,CustomDetrafReg,LongWritable,Text> {
		
		protected void reduce(LongWritable arg0, Iterable<CustomDetrafReg> arg1,
				org.apache.hadoop.mapreduce.Reducer.Context arg2)
				throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer(); 
			for(CustomDetrafReg reg : arg1) {
				sb.append(reg.toString()+"\t"); 
			}
			arg2.write(arg0, new Text(sb.toString()));
		}
	}
	
	
