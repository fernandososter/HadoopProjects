package ler.posicional;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RealizarSeek {

	
	public static void main(String...args) throws IOException {
		
		String fileUri = ""; 
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); 
		FSDataInputStream in = fs.open(new Path(fileUri)); 
		
		long currentPosition = in.getPos(); //posicao atual do offset.
		in.seek(0); // 0 volta para o inicio do arquivo.
					// qualquer outro numero direciona para a posicao do arquivo.
					// ABSOLUTE POSITION
		
		in.close();
	
	}
	
	
	
	
}
