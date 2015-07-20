package compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressionImplementation {

	
	/**
	 * DEFLATE org.apache.hadoop.io.compress.DefaultCodec
gzip org.apache.hadoop.io.compress.GzipCodec
bzip2 org.apache.hadoop.io.compress.BZip2Codec
LZO com.hadoop.compression.lzo.LzopCodec
LZ4 org.apache.hadoop.io.compress.Lz4Codec
Snappy org.apache.hadoop.io.compress.SnappyCodec
	 */
	
	
	public static void deflate() {
		
	}
	
	public static void gzip() {
	
	}
	
	
	public static void bzip2() {
		
	}
	
	public static void lzo() {
		
	}
	
	public static void lz4() {
		
	}
	
	public static void snappy() {
		
	}
	
	public static void compress(String codec) throws ClassNotFoundException, IOException {
		Class<?> codecClass = Class.forName(codec) ;
		Configuration conf = new Configuration(); 
		CompressionCodec compCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf); 
	
		CompressionOutputStream out = compCodec.createOutputStream(System.out); 
		IOUtils.copyBytes(System.in, out, 4096,false);
		out.finish();
	}
	
	
	public static void descobrirCodec() throws IllegalArgumentException, IOException {
		String fileUri = ""; 
		Configuration conf = new Configuration(); 
		
		FileSystem fs = FileSystem.get(URI.create(fileUri),conf); 
		
		CompressionCodecFactory factory = new CompressionCodecFactory(conf) ;
		CompressionCodec codec = factory.getCodec(new Path(fileUri)); 
		
		// para descompactar o arquivo
		
		String outputUri = CompressionCodecFactory.removeSuffix(fileUri, codec.getDefaultExtension()); 
		
		InputStream in = null; 
		OutputStream out = null; 
		
		try {
			in = codec.createInputStream(fs.open(new Path(fileUri))); 
			out = fs.create(new Path(outputUri)); 
			IOUtils.copyBytes(in, out, conf);
		} finally {
			IOUtils.closeStream(in);
		}
		
		
	}
	
	
	public static void usandoCodecPool() throws ClassNotFoundException, IOException {
		
		Class<?> codecClass = Class.forName(""); //codecclassname
		Configuration conf = new Configuration(); 
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf); 
		Compressor compressor = null; 
		try{
			compressor = CodecPool.getCompressor(codec);
			CompressionOutputStream out = codec.createOutputStream(System.out,compressor); 
			
			IOUtils.copyBytes(System.in, out, 4096,false);
			out.finish();
			
		}finally{
			CodecPool.returnCompressor(compressor);
		}
		
	}
	
	
}
