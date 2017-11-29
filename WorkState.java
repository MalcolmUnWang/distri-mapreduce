package map_reduce_demo;

public class WorkState {
	int state;
	public WorkState(){
		state = 0;
	}
	public boolean IsLeisure(){
		if(state == 0)
			return true;
		else
			return false;
	}
	public boolean IsMBusy(){
		if(state == 2)
			return true;
		else
			return false;
	}
	public boolean IsMReady(){
		if(state == 1)
			return true;
		else
			return false;
	}
	public boolean IsMWrong(){
		if(state == 3)
			return true;
		else
			return false;
	}
	public boolean IsRBusy(){
		if(state == 5)
			return true;
		else
			return false;
	}
	public boolean IsRReady(){
		if(state == 4)
			return true;
		else
			return false;
	}
	public boolean IsRWrong(){
		if(state == 6)
			return true;
		else
			return false;
	}
	public boolean IsFinished(){
		if(state == 7)
			return true;
		else
			return false;
	}
	public void SetasLeisure(){
		state = 0;
	}
	public void SetasMBusy(){
		state = 2;
	}
	public void SetasMReady(){
		state = 1;
	}
	public void SetasMWrong(){
		state = 3;
	}
	public void SetasRBusy(){
		state = 5;
	}
	public void SetasRReady(){
		state = 4;
	}
	public void SetasRWrong(){
		state = 6;
	}
	public void SetasFinished(){
		state = 7;
	}
	public int GetState(){
		return state;
	}
}
