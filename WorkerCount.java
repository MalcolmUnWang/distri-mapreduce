package map_reduce_demo;

public class WorkerCount {
	int MT,RT,mt,rt;
	public WorkerCount(){
		MT = 0;
		RT = 0;
		mt = 0;
		rt = 0;
	}
	public void set(int m,int r){
		MT = m;
		RT = r;
		mt = m;
		rt = r;
	}
	public int getMT(){
		return MT;
	}
	public int getRT(){
		return RT;
	}
	public int getmt(){
		return mt;
	}
	public int getrt(){
		return rt;
	}
	public void Submt(){
		mt--;
	}
	public void Subrt(){
		rt--;
	}
	public boolean misEmpty(){
		if(mt == 0)
			return true;
		else
			return false;
	}
	public boolean risEmpty(){
		if(rt == 0)
			return true;
		else
			return false;
	}
}
