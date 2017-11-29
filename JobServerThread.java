package map_reduce_demo;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class JobServerThread extends Thread{
	LinkedBlockingQueue<Job> jobqueue;
	Socket jobsocket; 
	WorkState ws;
	public JobServerThread(LinkedBlockingQueue<Job> jq,WorkState Ws){
		jobqueue = jq;
		ws = Ws;
	}
	public void run(){
		ArrayList<JobClientState> jcsarray;
		jcsarray = new ArrayList<JobClientState>();
		System.out.println("JobServerThread is started");
		ServerSocket serversocket = null;
		try {
			serversocket = new ServerSocket(6688);
		} catch (IOException e1) {
			// TODO 自动生成的 catch 块
			e1.printStackTrace();
		} 
		while(true){
		try {
			jobsocket = serversocket.accept();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		System.out.println("A new jobclient has been connected");
		JobClientState jcs = null;
		try {
			jcs = new JobClientState(jobsocket);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		jcsarray.add(jcs);
		JobServerThreadSon serverThread = new JobServerThreadSon(jcs,jobqueue,ws); 
		serverThread.start(); 
		}
	}
}
