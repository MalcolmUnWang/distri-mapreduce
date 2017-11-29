package map_reduce_demo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.LinkedBlockingQueue;

public class JobServerThreadSon extends Thread{
	JobClientState jcs;
	LinkedBlockingQueue<Job> jobqueue;
	WorkState ws;
	public JobServerThreadSon(JobClientState Jcs,LinkedBlockingQueue<Job> Jobqueue,WorkState Ws){
		jcs = Jcs;
		jobqueue = Jobqueue;
		ws = Ws;
	}
	public void run(){
		try{

			BufferedInputStream bis = new BufferedInputStream(jcs.GetSocket().getInputStream());
			ObjectInputStream ois = new ObjectInputStream(bis);
			ObjectOutputStream oos = new ObjectOutputStream(jcs.GetSocket().getOutputStream());
			Object obj = null;
			while(true){
				try {
					obj = ois.readObject();
				} catch (ClassNotFoundException e1) {
					e1.printStackTrace();
				}
				Job job = (Job) obj;
				JobBack back= new JobBack(ws.GetState());
				oos.writeObject(back);
				oos.flush();
				synchronized(jobqueue){
					jobqueue.add(job);
					job.fileReceive(jcs.GetSocket(),oos);
					job.fileReceive(jcs.GetSocket(),oos);
				synchronized(ws){
					if(ws.IsLeisure()){
						ws.SetasMReady();
					}
					if(ws.IsMReady()){
						ws.notifyAll();
					}
				}
			}
				
			}
		}catch (IOException e){
			e.printStackTrace();
		}
	} 
}
