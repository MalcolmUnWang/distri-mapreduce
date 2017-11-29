package map_reduce_demo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TaskServerHeartThread extends Thread{
	TaskClientState tcs;
	WorkerCount wc;
	WorkState ws;
	TaskHeart th;
	public TaskServerHeartThread(TaskClientState Tcs,WorkState Ws,WorkerCount w){ 
		tcs = Tcs;
		ws = Ws;
		wc = w;
	} 
	public void run(){ 
		try {
			DataInputStream dis = new DataInputStream(tcs.GetSocket().getInputStream());
			while(true){
				int x = dis.readInt();
				synchronized(tcs){
					if(x==1){
						synchronized(tcs){
							tcs.SetasReady();
						}
					}
					else if(x==2){
						synchronized(tcs){
							tcs.SetasBusy();
						}
					}
					else if(x==3){
		        		final int buffersize = 4096 * 5;
		    			long size = dis.readLong();
		    			byte[] buf = new byte[buffersize];
		    			File f = new File("bible" + (wc.getmt()-1) + ".mpd");
		    			DataOutputStream bos = new DataOutputStream(new FileOutputStream(f) );
		    			while(true){
		    				int read; 
		    				read = dis.read(buf,0,min(size,buffersize));
		    				size = size - read;
		    				bos.write(buf,0,read);
		    				if(size <= 0 ){
		    					break;
		    				}
		    			bos.flush();
		    			}
		    			bos.close();
						synchronized(tcs){
							tcs.SubaMW();
							if(tcs.getmwc() == 0){
								tcs.SetasReady();
							}
						}
						synchronized(wc){
							wc.Submt();
							if(wc.misEmpty()){
								ws.SetasRReady();
								synchronized(ws){
									ws.notifyAll();
								}
							}
						}
					}
					else if(x==4){
						synchronized(tcs){
							tcs.SetasBusy();
						}
					}
					else if(x==5){
		        		final int buffersize = 4096 * 5;
		    			long size = dis.readLong();
		    			byte[] buf = new byte[buffersize];
		    			File f = new File("bible" + (wc.getrt()-1) + ".rdc");
		    			DataOutputStream bos = new DataOutputStream(new FileOutputStream(f) );
		    			while(true){
		    				int read; 
		    				read = dis.read(buf,0,min(size,buffersize));
		    				size = size - read;
		    				bos.write(buf,0,read);
		    				if(size <= 0 ){
		    					break;
		    				}
		    			bos.flush();
		    			}
		    			bos.close();
						synchronized(tcs){
							tcs.SubaRW();
							if(tcs.getmwc() == 0){
								tcs.SetasReady();
							}
						}
						synchronized(wc){
							wc.Subrt();
							if(wc.misEmpty()){
								ws.SetasFinished();
								synchronized(ws){
									ws.notifyAll();
								}
							}
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private int min(long size,int y) {
		if(size<y)
			return (int) size;
		else
			return y;
	}
}
