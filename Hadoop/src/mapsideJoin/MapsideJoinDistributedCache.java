package mapsideJoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


class MapsideJoinMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	
	private Hashtable<String,String> ht = null; 
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String anexo5Line = null; 
		
		
		String[] arr = value.toString().split(";"); 
		
		
		
		
		if(ht.containsKey(arr[1]) ) {
			anexo5Line = ht.get(arr[1]); 
			Text t = new Text(); 
			t.set(value.toString() + " - " + anexo5Line);
			
		//fazer o que tem que ser feito
			context.write(key, t);
		}
		
		
		
	}
	
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration()); 
		
		URI[] uris = context.getCacheFiles(); 
			
		ht = new Hashtable<String,String>(); 
		
		for(URI uri : uris) {
			Path p = new Path(uri.toString()); 
			
			File f = new File(p.getName()); 
			
			BufferedReader br = new BufferedReader(new FileReader(f)); 
			
			String str = null; 
			while( ( str = br.readLine()) != null ) {
				String[] values = str.split(";"); 
				ht.put(new String(values[1]), str); 
			}
		}
	}


}






public class MapsideJoinDistributedCache extends Configured implements Tool {

	

	@Override
	public int run(String[] arg0) throws Exception {
		
		FileSystem fs = FileSystem.get(new URI("hdfs://bigtop1.vagrant:8020/user/fsoster/output/teste3/"), getConf()); 

		Path p = new Path("hdfs://bigtop1.vagrant:8020/user/fsoster/output/teste3"); 
		
		if(fs.exists(p)) {
			fs.delete(p); 
		}


		

		Job job = new Job(getConf()); 
		job.setJarByClass(this.getClass());
		
		//bin/hadoop fs -copyFromLocal mylib.jar /user/hduser/anexo5.csv
		// DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
	//	DistributedCache.addCacheFile(new URI("hdfs://bigtop1.vagrant:8020/user/hduser/input/anexo5/anexo5.csv"), getConf());
	//	DistributedCache.addCacheArchive(new URI("/user/hduser/anexo5.tar.gz"), getConf());
	//	job.setUser("hduser");
		//FileOutputFormat.
		job.setMapperClass(MapsideJoinMapper.class);
		job.addCacheFile(new URI("hdfs://bigtop1.vagrant:8020/user/hduser/input/caralho/caralho.csv"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://bigtop1.vagrant:8020/user/hduser/input/detraf/DETRAF_FINAL_201408_HOME_TIM_C_20140902062129_ITX.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://bigtop1.vagrant:8020/user/fsoster/output/teste3/"));
		
		return job.waitForCompletion(true)?0:1; 
		
	}

	
	public static void main(String...args) throws Exception {
		ToolRunner.run(new MapsideJoinDistributedCache() , args); 
	}
	
}
