package customWritable;

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

public class CustomWritableDriver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(getConf()); 
		
		job.setJarByClass(CustomWritableDriver.class);
		job.setJobName("CustomWritableDriver");
		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:9000/GovBr/*"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/DETRAF/output/output"));
	
		job.setMapperClass( CustomMapper.class);
		job.setReducerClass( CustomReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(CustomDetrafReg.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(CustomDetrafReg.class);
		
		return (job.waitForCompletion(true) ? 0 : 1); 
	}
	
	
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Configuration(), new CustomWritableDriver(),args);
		System.exit(exit); 
	}
	

	
	
	
	
	
}


 class CustomMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
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
 
  class CustomReducer extends Reducer<LongWritable,CustomDetrafReg,LongWritable,Text> {
		
	@Override
	protected void reduce(LongWritable arg0, Iterable<CustomDetrafReg> arg1,
			Reducer<LongWritable, CustomDetrafReg, LongWritable, Text>.Context arg2)
					throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer(); 
		for(CustomDetrafReg reg : arg1) {
			sb.append(reg.toString()+"\t"); 
		}
		arg2.write(arg0, new Text(sb.toString()));
	}  
  }
