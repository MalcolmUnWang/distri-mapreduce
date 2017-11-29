package map_reduce_demo;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskServerThread extends Thread{
	Queue<Job> jobqueue;
	ArrayList<TaskClientState> tcsarray;
	WorkState ws;
	public TaskServerThread(LinkedBlockingQueue<Job> jq , WorkState w){
		jobqueue = jq;
		ws = w;
	}
	public void run(){
		try {
			WorkerCount wc = new WorkerCount();
			tcsarray = new ArrayList<TaskClientState>();
			TaskControlThread tct =new TaskControlThread(jobqueue, tcsarray, ws, wc);
			tct.start();
			System.out.println("TaskServerThread is started");
			ServerSocket serverSocket = new ServerSocket(8888); 
			Socket socket = null; 
			TaskClientState tcs =null ;
			while (true){ 
				socket = serverSocket.accept();
				System.out.println("A new taskclient has been connected");
				tcs = new TaskClientState(socket);
				synchronized(tcsarray){
					if(tcsarray.isEmpty())
						tcsarray.notifyAll();
					tcsarray.add(tcs);
				}
				TaskServerHeartThread serverThread = new TaskServerHeartThread(tcs,ws,wc); 
			    serverThread.start(); 
			} 
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
