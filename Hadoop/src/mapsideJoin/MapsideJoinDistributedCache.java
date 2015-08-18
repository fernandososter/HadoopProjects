package mapsideJoin;

import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


class MapsideJoinMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	
	private Hashtable<String,String> ht = null; 
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String anexo5Line = null; 
		
		
		if(ht.containsKey(key.toString()) ) {
			anexo5Line = ht.get(key.toString()); 
		}
		
		
	//fazer o que tem que ser feito
		context.write(key, value);
	}
	
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration()); 
		ht = new Hashtable<String,String>(); 
		for(Path p: paths) {
			if(p.getName().equals("anexo5.csv")) {
				FSDataInputStream fs = p.getFileSystem(context.getConfiguration()).open(p); 
				String[] lines = fs.toString().split("\n"); 
				for(String str: lines) {
					String[] values = str.split(";"); 
					ht.put(new String(values[0]), str); 
				}
			}
		}
	}


}






public class MapsideJoinDistributedCache extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		
		Job job = new Job(getConf()); 
		job.setJarByClass(this.getClass());
		
		//bin/hadoop fs -copyFromLocal mylib.jar /user/hduser/anexo5.csv
		// DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
		DistributedCache.addCacheFile(new URI("/user/hduser/anexo5.csv"), getConf());
		DistributedCache.addCacheArchive(new URI("/user/hduser/anexo5.tar.gz"), getConf());
		
		
		return job.waitForCompletion(true)?0:1; 
		
	}

	
	public static void main(String...args) throws Exception {
		ToolRunner.run(new MapsideJoinDistributedCache() , args); 
	}
	
}
