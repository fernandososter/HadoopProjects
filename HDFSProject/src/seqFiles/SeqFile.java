package seqFiles;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SeqFile {

	
	public static void main(String...args) throws Exception {
		
		String uri = "C:/temp/teste.seq"; 
		
		
		Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(URI.create(uri), conf); 
		Path path = new Path(uri); 
		
		
		
		
		
	}

	
	public static void readComMarcador(Configuration conf, FileSystem fs, Path path) throws Exception {
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf); 
		
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); 
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf); 
		
		
		long position = reader.getPosition(); 
		
		while(reader.next(key, value)) {
			
			String syncSeen = reader.syncSeen()? "*" : ""; 
			reader.seek(5); // a partir da linha 5, o primeiro marcador
			System.out.println(position + syncSeen + " : " + key.toString() + " : " + value.toString() );
			position = reader.getPosition(); 
			
		}
		
		IOUtils.closeStream(reader);
	}
	
	
	
	public static void read(Configuration conf, FileSystem fs, Path path) throws Exception {
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf); 
		
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); 
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf); 
		
		Long position = reader.getPosition(); 
		
		
		while(reader.next(key, value)) {
			String syncSeen = reader.syncSeen()? "*" : ""; //verifica se eh marcador
			System.out.println(position + syncSeen + " : " + key.toString() + " : " + value.toString() );
			position = reader.getPosition(); 
		}
		
		IOUtils.closeStream(reader);
	}
	
	
	
	
	public static void write(Configuration conf, FileSystem fs, Path path) throws Exception {
		SequenceFile.Writer writer = 
				SequenceFile.createWriter(fs, conf, path, LongWritable.class, Text.class); 
		writer.append(new LongWritable(1), new Text("1"));
		writer.close();
	}
}
