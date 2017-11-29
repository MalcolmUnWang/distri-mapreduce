package map_reduce_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;

public class TaskControlThread extends Thread{
	Queue<Job> jobqueue;
	ArrayList<TaskClientState> tcsarray;
	WorkState ws;
	WorkerCount wc;
	public TaskControlThread(Queue<Job> jq,ArrayList<TaskClientState> tcsa,WorkState w,WorkerCount Wc){
		jobqueue = jq;
		tcsarray = tcsa;
		ws = w;
		wc = Wc;
	}
	public void run(){
		synchronized(ws){
			
			while(true){
				while(ws.IsLeisure() || ws.IsMBusy() || ws.IsRBusy()){
					try {
						ws.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if(ws.IsMReady()){
					wc.set(tcsarray.size(),tcsarray.size()/*jobqueue.peek().GetMT(),jobqueue.peek().GetMT()*/);
					FileOperations.separateFile(jobqueue.peek().getDataName(),tcsarray.size() );
					synchronized(tcsarray){
						while(tcsarray.isEmpty()){
							try {
								tcsarray.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						for(TaskClientState tcs : tcsarray){
							Start st =new Start(tcs,jobqueue.peek().getFileName());
							st.start();
						}
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						int i = 0;
						for(int count = wc.getMT();count > 0;){
							for(TaskClientState tcs : tcsarray){
								count--;
								String file = jobqueue.peek().getDataName();
								int index = file.lastIndexOf(".");
								String fname = file.substring(0, index);
								String extend = file.substring(index+1);
								AddaMapper adm = new AddaMapper(tcs,fname + i + "."+extend);
								i = i + 1;
								adm.start();
								if(count <= 0){
									break;
								}
							}
						}
					}
					ws.SetasMBusy();
				}
				else if(ws.IsRReady()){
					String  [] fpath = new String[tcsarray.size()];
					for(int i = 0; i<tcsarray.size();i++)
						fpath[i] = "bible"+ i+".mpd";
					FileOperations.mergeFiles(fpath,"bible_merge.txt");
					System.out.println("Merged!");
					File f = new File(jobqueue.peek().getFileName());
					FileInputStream fis;
					try {
						fis = new FileInputStream(f);
						FileClassLoader cl =new FileClassLoader(fis);
					    Class c = cl.loadClass("mapreduce.test.MyMapp");
					    Reducer m = new Reducer();
				        m.serverCombine("bible_merge.txt", c);
				        fis.close();
				        System.out.println("Server Combining has been done!\n Reducing...");

						FileOperations.separateFile("bible_merge.scb",tcsarray.size());
					} catch (FileNotFoundException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					int i = 0;
					for(int count = wc.getRT();count > 0;){
						for(TaskClientState tcs : tcsarray){
							count--;
							AddaReducer adr = new AddaReducer(tcs,"bible_merge" + i + ".scb");
							i++;
							adr.start();
							if(count <= 0){
								break;
							}
						}
					}
					ws.SetasMBusy();
				}
				else if(ws.IsFinished()){
					String  [] fpath = new String[tcsarray.size()];
					for(int i = 0; i<tcsarray.size();i++)
						fpath[i] = "bible"+ i+".rdc";
					FileOperations.mergeFiles(fpath,"bible_merge.rdc");
					System.out.println("Merged!");
					jobqueue.poll();
					if(jobqueue.isEmpty())
						ws.SetasLeisure();
					else
						ws.SetasMReady();
				}
			} 
		}
	}
}
