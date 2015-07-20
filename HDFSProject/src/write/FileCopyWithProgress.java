package write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {

	public static void main(String...args) throws IOException {
		String localSrc = args[0]; 
		String dst = args[1]; 
		
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc)); 
		Configuration conf = new Configuration(); 
		
		FileSystem fs = FileSystem.get(URI.create(dst),conf); 
		OutputStream ou = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.println(".");
			}
		}); 
		
		IOUtils.copyBytes(in, System.out,4096,false);
	
		
	}
	
}
