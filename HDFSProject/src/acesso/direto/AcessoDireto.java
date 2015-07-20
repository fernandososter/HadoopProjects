package acesso.direto;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class AcessoDireto {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String...args) throws MalformedURLException, IOException {
		InputStream in = null; 
		try {
			in = new URL("hdfs://localhost:54310/DETRAF/input/DETRAF_FINAL_201411_HOME_OI_SA_C_20141202081434_ITX.csv").openStream(); 
			IOUtils.copyBytes(in, System.out,4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
	
	
	
}
