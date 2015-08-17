package seqFiles;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;

public class MapFiles {

	
	public static void main(String...args) throws Exception {
		String uri = ""; 
		
		Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(uri), conf); 
		Path path = new Path(uri); 
		
	}
	

	
	
	public static void write(Configuration conf, FileSystem fs, Path path) throws IOException {
		
		MapFile.Writer writer = new MapFile.Writer(conf, fs, path.toString(), LongWritable.class, Text.class); 
		
		writer.append(new LongWritable(1), new Text("1"));
		//outras insercoes devem ser em ordem (sort).
		writer.close();
		
	}
	
	
	
	public static void read(Configuration conf, FileSystem fs, Path path) throws Exception {
		
		MapFile.Reader reader = new MapFile.Reader(fs, path.toString(), conf); 
		WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
		
		while(reader.next(key, value)) {
			
			System.out.println(key + ":" + value);
			
		}
		
		IOUtils.closeStream(reader);
	}
	
}
