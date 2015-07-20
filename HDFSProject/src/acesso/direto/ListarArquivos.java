package acesso.direto;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListarArquivos {

	public static void main(String...args) throws MalformedURLException, IOException {
		
		String path = ""; 
		Configuration conf = new Configuration(); 
	
		FileSystem fs = FileSystem.get(conf); 
		
		FileStatus[] list = fs.listStatus(new Path(path)); //folder ou arquivo. se arquivo retorna 1 se folder retorna
										//varios
		for(FileStatus f : list) {
			System.out.println(f.toString());
		}
		
		fs.close();
	}
	
	
	
}
