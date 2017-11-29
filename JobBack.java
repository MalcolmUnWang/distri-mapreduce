package  map_reduce_demo;
import java.io.Serializable;

public class JobBack implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4273286997865617136L;
	private int number;
	public JobBack(){
		number = 0;
	}
	public JobBack(int n){
		number = n;
	}
	public void setNumber(int n){
		number = n;
	}
	public int getNumber(){
		return number;
	}
}
