package teste;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TesteMapper extends Mapper<LongWritable,Text,LongWritable,Text>{

	
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String[] tkns = value.toString().split(";"); 
		LongWritable eot = new LongWritable(Long.parseLong(tkns[0])); 
	//	Text t = new Text(tkns[6]+";"+tkns[7]+";"+tkns[14]); 
		context.write(eot, value);
	}
}
