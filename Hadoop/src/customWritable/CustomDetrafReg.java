package customWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomDetrafReg implements Writable{

	//6;451;201305;201304;5105101H;0; LENV;N;4;4,7;0,3572;1,68;0,06;0; 1,74
	
	private int eotOrigem;
	private int eotDestino;
	private String periodo1;
	private String periodo2;
	private String descritor; 
	private Double valor; 

	public int getEotOrigem() {
		return eotOrigem;
	}

	public void setEotOrigem(int eotOrigem) {
		this.eotOrigem = eotOrigem;
	}

	public int getEotDestino() {
		return eotDestino;
	}

	public void setEotDestino(int eotDestino) {
		this.eotDestino = eotDestino;
	}

	public String getPeriodo1() {
		return periodo1;
	}

	public void setPeriodo1(String periodo1) {
		this.periodo1 = periodo1;
	}

	public String getPeriodo2() {
		return periodo2;
	}

	public void setPeriodo2(String periodo2) {
		this.periodo2 = periodo2;
	}

	public String getDescritor() {
		return descritor;
	}


	public void setDescritor(String descritor) {
		this.descritor = descritor;
	}





	public Double getValor() {
		return valor;
	}





	public void setValor(Double valor) {
		this.valor = valor;
	}





	public void parser(String[] tokens) {
		
		this.eotOrigem = Integer.parseInt(tokens[0]) ;
		this.eotDestino = Integer.parseInt(tokens[1]) ;
		this.periodo1 = tokens[2]; 
		this.periodo2 = tokens[3];
		this.descritor = tokens[6];
		this.valor = Double.parseDouble(tokens[14].replaceAll(",", ".")); 
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.eotOrigem = arg0.readInt(); 
		this.eotDestino = arg0.readInt(); 
		this.periodo1 = arg0.readUTF(); 
		this.periodo2 = arg0.readUTF(); 
		this.descritor = arg0.readUTF(); 
		this.valor = arg0.readDouble(); 
		
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.eotOrigem);
		arg0.writeInt(this.eotDestino);
		arg0.writeUTF(this.periodo1);
		arg0.writeUTF(this.periodo2);
		arg0.writeUTF(this.descritor);
		arg0.writeDouble(this.valor);
	}

	@Override
	public String toString() {
		return this.eotOrigem + " - " + this.eotDestino + " - " +
	this.periodo1 + " - " + this.periodo2 + " - " + this.descritor + " - " +
				this.valor; 
	}
}
