package map_reduce_demo;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientState {
	Socket socket;
	int state;
	DataOutputStream Dos;
	public ClientState(Socket so) throws IOException{
		socket =so;
		state = 0;
		Dos = new DataOutputStream(so.getOutputStream());
	}
	public DataOutputStream getodos(){
		return Dos;
	}
	public Socket GetSocket(){
		return socket;
	}
	public int GetState(){
		return state;
	}
	public boolean IsLeisure(){
		if(state == 0)
			return true;
		else
			return false;
	}
	public boolean IsBusy(){
		if(state == 2)
			return true;
		else
			return false;
	}
	public boolean IsReady(){
		if(state == 1)
			return true;
		else
			return false;
	}
	public void SetasLeisure(){
		state = 0;
	}
	public void SetasBusy(){
		state = 2;
	}
	public void SetasReady(){
		state = 1;
	}
}
