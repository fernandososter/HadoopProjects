package mapsideJoin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


class CombineValuesMapper extends Mapper<Text,TupleWritable,NullWritable,Text> { 
	
	private static final NullWritable nullKey = NullWritable.get(); 
	private String separator; 
	private StringBuilder valuebuilder = new StringBuilder(); 
	private Text outValue = new Text(); 
	
	
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		separator = context.getConfiguration().get("separator"); 
	}
	
	@Override
	protected void map(Text key, TupleWritable value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		valuebuilder.append(key).append(separator); 
		for(Writable writable : value) {
			valuebuilder.append(writable.toString()).append(separator) ;
		}
		valuebuilder.setLength(valuebuilder.length() - 1);
		outValue.set(valuebuilder.toString()); // text é até 2Gb? ???
		context.write(nullKey, outValue);
		valuebuilder.setLength(0);
	}
}

class SortByKeyMapper extends Mapper<LongWritable,Text,Text,Text> {
	 private int keyIndex;
	private Splitter splitter; // <- guava
	private Joiner joiner;  // <- guava
	 private Text joinKey = new Text();
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String separator = context.getConfiguration().get("separator"); 
		keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex")); 
		splitter = Splitter.on(separator); 
		joiner = Joiner.on(separator) ;
		
	
	}
	
	
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Iterable<String> values = splitter.split(value.toString()); 
		joinKey.set(Iterables.get(values, keyIndex));
		if(keyIndex != 0) {
			value.set(reorderValue(values, keyIndex));
		}
		context.write(joinKey, value);
	}
	
	
	private String reorderValue(Iterable<String> values, int index) {
		List<String> temp = Lists.newArrayList(values) ;
		String originalFirst = temp.get(0); 
		String newFirst = temp.get(index); 
		temp.set(0, newFirst); 
		temp.set(index,originalFirst); 
		return joiner.join(temp); 
	}
	
}

class SortByKeyReducer extends Reducer<Text,Text,NullWritable,Text> {
	private static final NullWritable nullKey = NullWritable.get(); 
	protected void reduce(Text arg0, Iterable<Text> arg1,
			org.apache.hadoop.mapreduce.Reducer.Context arg2)
			throws IOException, InterruptedException {
		// o reducer apenas escreve. 
		for(Text value : arg1) { arg2.write(nullKey, value); } 
	}
}

class MSJPartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return 0;
	}
}

public class MapSideJoin extends Configured implements Tool{

	public static void main(String...args) throws Exception {
		System.exit(ToolRunner.run(new MapSideJoin() , args));
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String separator = ";";
		String keyIndex = "0";
		int numReducers = 10; 
	
		
		String jobOneInputPath = ""; // 1 inputpath
        String jobTwoInputPath = ""; //2 inputpath
        String joinJobOutPath = ""; // output path

        String jobOneSortedPath = jobOneInputPath + "_sorted";
        String jobTwoSortedPath = jobTwoInputPath + "_sorted";
		
        Configuration conf1 = new Configuration(); 
        conf1.set("separator",separator);
        conf1.set("keyIndex", keyIndex);
        Configuration conf2 = new Configuration(); 
        conf2.set("separator",separator);
        conf2.set("keyIndex", keyIndex);
        
        Job firstSort = Job.getInstance(conf1); 
        firstSort.setJar("FirstSort");
        FileInputFormat.addInputPath(firstSort, new Path(jobOneInputPath));
        FileOutputFormat.setOutputPath(firstSort, new Path(jobOneSortedPath));
        firstSort.setMapperClass(SortByKeyMapper.class);
        firstSort.setNumReduceTasks(numReducers);
        
      
        Job secondSort = Job.getInstance(conf2); 
        firstSort.setJar("SecondSort");
        FileInputFormat.addInputPath(secondSort, new Path(jobTwoInputPath));
        FileOutputFormat.setOutputPath(secondSort, new Path(jobTwoSortedPath));
        secondSort.setMapperClass(SortByKeyMapper.class);
        secondSort.setNumReduceTasks(numReducers);
        
        Job mapJoin = Job.getInstance(getMapJoinConfiguration(separator,jobOneSortedPath,jobTwoSortedPath)); 
        mapJoin.setInputFormatClass(CompositeInputFormat.class);
        
        List<Job> jobs = Lists.newArrayList(firstSort, secondSort); 
        
        for(Job job : jobs) {
        	boolean status = job.waitForCompletion(true); 
        	if(!status) {break;} 
        }
	
		return 1; 
	}
	
	
	
	
	private static Configuration getMapJoinConfiguration(String separator, String...paths) {
		
		Configuration config = new Configuration(); 
		config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", separator);
		String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,paths);
		config.set("maped.join.expr",joinExpression);
		config.set("separator", separator);
		return config; 
	}
	
}
