package customInputOutput;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


class DETRAFKeyPair implements WritableComparable<DETRAFKeyPair> {
	String eot1; 
	String eot2; 
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.eot1 = arg0.readUTF(); 
		this.eot2 = arg0.readUTF(); 
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(eot1);
		arg0.writeUTF(eot2);
	}
	@Override
	public String toString() {
		return "DETRAFKeyPair [eot1=" + eot1 + ", eot2=" + eot2 + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((eot1 == null) ? 0 : eot1.hashCode());
		result = prime * result + ((eot2 == null) ? 0 : eot2.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DETRAFKeyPair other = (DETRAFKeyPair) obj;
		if (eot1 == null) {
			if (other.eot1 != null)
				return false;
		} else if (!eot1.equals(other.eot1))
			return false;
		if (eot2 == null) {
			if (other.eot2 != null)
				return false;
		} else if (!eot2.equals(other.eot2))
			return false;
		return true;
	}
	@Override
	public int compareTo(DETRAFKeyPair arg0) {
		// TODO Auto-generated method stub
		int resultado = this.eot1.compareTo(arg0.eot1); 
		if(resultado == 0) {
			return this.eot2.compareTo(arg0.eot2); 
		}
		return resultado;
	}
}



class REALIZADOKey implements WritableComparable<REALIZADOKey> {
	String eot; 
	String tecnologia; 
	String canal; 
	Character movimento; 
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.eot = arg0.readUTF(); 
		this.tecnologia = arg0.readUTF(); 
		this.canal = arg0.readUTF(); 
		this.movimento = arg0.readChar(); 
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(this.eot);
		arg0.writeUTF(this.tecnologia);
		arg0.writeUTF(this.canal);
		arg0.writeChar(this.movimento);
	}
	@Override
	public int compareTo(REALIZADOKey o) {
		// TODO Auto-generated method stub
		int resultado = this.eot.compareTo(o.eot); 
		if(resultado == 0) {
			resultado = this.tecnologia.compareTo(o.tecnologia); 
			if(resultado ==0) {
				resultado = this.canal.compareTo(o.canal);
				if(resultado ==0) {
					return this.movimento.compareTo(o.movimento); 
				}
			}
		}
		return resultado;
	}
}


class REALIZADOCustomInputFormat extends FileInputFormat<REALIZADOKey,DoubleWritable> {

	@Override
	public RecordReader<REALIZADOKey, DoubleWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new REALIZADOCustomRecordReader();
	}
	
}

class REALIZADOCustomRecordReader extends RecordReader<REALIZADOKey,DoubleWritable> {
	InputStream is; 
	REALIZADOKey key; 
	DoubleWritable value; 
	
	String[] lines; 
	int iteration = 0 ; 
	
	
	@Override
	public void close() throws IOException {
		if(is != null) {
			is.close();
		}
	}

	@Override
	public REALIZADOKey getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		
		FileSplit fs = (FileSplit) arg0; 
 		FileSystem fSystem = fs.getPath().getFileSystem(arg1.getConfiguration()); 
		FSDataInputStream data = fSystem.open(fs.getPath()); 
		lines = data.toString().split("\n"); 
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//SMS - 001026 INTERVASDADOSPOS  VC3 MC 2 02 CLAC9001  CON3G  031222817000000000000.00000000000.10D
		//MMS - 001003 INTERVASDADOSPRE  VC1 MC 2 02 CLAC9001  CONGSM 031222817000000000000.00000000253.45D
		//ITX - 001011 INTEROUTCLPO      0300MF 1 02 CLAC9001  CONGSM 041222834000000000003.50000000001.15C


		String line = lines[iteration] ;
		this.key = new REALIZADOKey(); 
		this.key.canal = line.substring(26,29); 
		this.key.eot = line.substring(3,6); 
		this.key.tecnologia = line.substring(8,25);
		this.key.movimento = line.charAt(91); 
		
		iteration +=1; 
		
		if(iteration <= lines.length)
			return true; 
		
		return false;
	}
}


class DETRAFCustomInputFormat extends FileInputFormat<DETRAFKeyPair,DoubleWritable>{
	@Override
	public RecordReader<DETRAFKeyPair, DoubleWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new DETRAFCustomRecordReader();
	}
}

class DETRAFCustomRecordReader extends RecordReader<DETRAFKeyPair,DoubleWritable> {
	InputStream is; 
	DETRAFKeyPair key; 
	DoubleWritable value; 
	String[] lines; 
	int iteration = 0; 
	int positions = 15; 
	
	@Override
	public void close() throws IOException {
		if(is != null) {
			is.close();
		}
	}

	@Override
	public DETRAFKeyPair getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) arg0; 
		Configuration conf = arg1.getConfiguration(); 
		Path path = fs.getPath(); 
		FileSystem fSystem = path.getFileSystem(conf); 		
		FSDataInputStream data = fSystem.open(fs.getPath()); 
		lines = data.toString().split("\n"); 
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arrStrs = lines[iteration].split(";"); 
		
		this.key = new DETRAFKeyPair(); 
		this.key.eot1 =  arrStrs[0]; 
		this.key.eot2 = arrStrs[1]; 
		
		this.value = new DoubleWritable(new Double(arrStrs[0].replaceAll(", ", "."))); 
		this.iteration +=1; 
		
		if(iteration <= lines.length) {
			return false; 
		}
		return true;
	}
	
}


class DETRAFMapper extends Mapper<DETRAFKeyPair,DoubleWritable,DETRAFKeyPair,DoubleWritable> {
	
	@Override
	protected void map(DETRAFKeyPair key, DoubleWritable value,
			Mapper<DETRAFKeyPair, DoubleWritable, DETRAFKeyPair, DoubleWritable>.Context context)
					throws IOException, InterruptedException {
		context.write(key, value);
	}
}

class REALIZADOMapper extends Mapper<REALIZADOKey,DoubleWritable,REALIZADOKey,DoubleWritable> {
	@Override
	protected void map(REALIZADOKey key, DoubleWritable value,
			Mapper<REALIZADOKey, DoubleWritable, REALIZADOKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(key, value);
	}
}



public class CustomIOHadoop  extends Configured implements Tool{
	

static {
	Configuration.addDefaultResource("/etc/hadoop/cluster-bigtop/hdfs-site.xml");
	Configuration.addDefaultResource("/etc/hadoop/cluster-bigtop/core-site.xml");
	Configuration.addDefaultResource("/etc/hadoop/cluster-bigtop/mapred-site.xml");
	Configuration.addDefaultResource("/etc/hadoop/cluster-bigtop/yarn-site.xml");
}
	
	
	public static void main(String...args) throws Exception {
		ToolRunner.run(new CustomIOHadoop(), args); 
	}

	@Override
	public int run(String[] arg0) throws Exception {
	
		Path outputPath = new Path("/user/hduser/outputTeste/"); 
		
		FileSystem fs = FileSystem.get(getConf()); 
		
		if(fs.exists(outputPath) ) {
			fs.removeAcl(outputPath);
		}
		
		
		Job job = new Job(getConf()); 
		job.setJarByClass(this.getClass());
		job.setUser("hduser");
		MultipleInputs.addInputPath(job, new Path("/user/hduser/input/detraf/"), 
				DETRAFCustomInputFormat.class,DETRAFMapper.class);
		MultipleInputs.addInputPath(job, new Path("/user/hduser/input/realizado/"), 
				REALIZADOCustomInputFormat.class,REALIZADOMapper.class);
			
		
		return job.waitForCompletion(true)?0:1;
	}
	
	
}
