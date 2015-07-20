package write;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileWriteComFlushes {

	
	public static void main(String...args) throws IOException {
		String uriDestino = ""; 
		String uriOrigem = ""; 
		
		
		InputStream in =  new BufferedInputStream(new FileInputStream(uriOrigem));  
		Configuration conf = new Configuration(); 
		
		
		FileSystem fs = FileSystem.get(URI.create(uriDestino), conf); 
		FSDataOutputStream out = fs.create(new Path(uriDestino)); 
		
		
		IOUtils.copyBytes(in, System.out,4096,false);
		

		out.hflush(); // hflush vai efetivar a gravacao e os outros usuarios ja poderao ver.
					   // no entanto essa gravacao pode ser na memoria do datanode, e nao no disco. 
		
		
		out.hsync(); // hsync vai garantir a gravacao no disco. Ou seja, se o datanode cair, 	
					// a informacao nao sera perdida. 
		
		
		
	}
	
	
	
}
