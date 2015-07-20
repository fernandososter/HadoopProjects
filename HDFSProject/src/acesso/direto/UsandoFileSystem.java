package acesso.direto;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class UsandoFileSystem {

	
	public static void main(String...args) throws IOException {
		String uri = "hdfs://localhost:54310/DETRAF/input/DETRAF_FINAL_201411_HOME_OI_SA_C_20141202081434_ITX.csv";
		
		Configuration conf = new Configuration(); 
		
		FileSystem fs = FileSystem.get(URI.create(uri),conf); 
		
		InputStream in = null; 
		try {
			in = fs.open(new Path(uri)); 
			IOUtils.copyBytes(in, System.out, 4096,false);
		} finally{
			IOUtils.closeStream(in);
		}
		
	}
	
	
	
}
