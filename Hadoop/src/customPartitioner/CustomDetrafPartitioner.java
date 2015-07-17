package customPartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomDetrafPartitioner extends Partitioner<LongWritable, Text> {

	
	
	/*
	 *Metodo que faz a quebra pelo numero de partitions. 
	 *Primeiro parametro é a chave. 
	 *Segundo param é o valor da linha (podemos fazer parser e usar um determinado 
	 *parametro. 
	 *Terceiro é o numero de reduce tasks rodando. 
	 *
	 *Condicao de retorno:
	 * 0 % numReduceTasks
	 * 1 % numReduceTasks
	 * 2 % numReduceTasks
	 * n % numReduceTasks
	 **/
	@Override
	public int getPartition(LongWritable key, Text value, int numReduceTasks) {
		
		if(key.get() < 300) {
			return 0% numReduceTasks; 
		} else if(key.get() <= 300 && key.get() <= 600) {
			return 1% numReduceTasks; 
		} else {
			return 2% numReduceTasks;
		}
		
	}

}
