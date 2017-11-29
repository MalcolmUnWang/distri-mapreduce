package map_reduce_demo;

import java.io.Serializable;

public class TaskHeart implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4046364333684643064L;
	/**
	 * 
	 */
	int state;
	public TaskHeart(){
		state = 0;
	}
	public TaskHeart(int x){
		state = x;
	}
	public void SetHeart(int x){
		state = x;
	}
	public boolean IsNormal(){
		if(state == 0)
			return true;
		else
			return false;
	}
	public boolean IsMStart(){
		if(state == 1)
			return true;
		else
			return false;
	}
	public boolean IsMWork(){
		if(state == 2)
			return true;
		else
			return false;
	}
	public boolean IsMEnd(){
		if(state == 3)
			return true;
		else
			return false;
	}
	public boolean IsRStart(){
		if(state == 4)
			return true;
		else
			return false;
	}
	public boolean IsRWork(){
		if(state == 5)
			return true;
		else
			return false;
	}
	public boolean IsREnd(){
		if(state == 6)
			return true;
		else
			return false;
	}
}
