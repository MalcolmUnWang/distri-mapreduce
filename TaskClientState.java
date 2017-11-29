package map_reduce_demo;

import java.io.IOException;
import java.net.Socket;

public class TaskClientState extends ClientState{
	int mwc,rwc;
	public TaskClientState(Socket so) throws IOException {
		super(so);
		mwc = 0;
		rwc = 0;
	}
	public int getmwc(){
		return mwc;
	}
	public int getrwc(){
		return rwc;
	}
	public void AddaMW(){
		mwc ++;
	}
	public void AddaRW(){
		rwc ++;
	}
	public void SubaMW(){
		mwc --;
	}
	public void SubaRW(){
		rwc --;
	}
}
