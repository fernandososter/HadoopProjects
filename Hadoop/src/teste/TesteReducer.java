package teste;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TesteReducer extends Reducer<LongWritable,Text,LongWritable,Text>{

	
	protected void reduce(LongWritable arg0, Iterable<Text> arg1,
			org.apache.hadoop.mapreduce.Reducer.Context arg2)
			throws IOException, InterruptedException {
		arg2.write(arg0, arg1);
		
	}
	
}
