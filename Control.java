package map_reduce_demo;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public class Control extends Thread{
	TaskClientState tcs;
	String dfn;
	int signal = 0;
	public Control(TaskClientState t,String d){
		tcs = t;
		dfn = d;
	}
	public void run(){
		synchronized(tcs){
			try {
				DataOutputStream dos = tcs.getodos();
				dos.writeInt(signal);
				File file = new File(dfn);
				DataPipeThread dpt = new DataPipeThread(file , dos);
				dpt.run();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		
		}
		
	}
}
