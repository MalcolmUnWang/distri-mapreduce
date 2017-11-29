package map_reduce_demo;

import java.io.*; 
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*; 
  
public class TaskTracker {
	public static void main(String[] args) { 
		try {
	        Socket socket = new Socket("localhost",8888); 
	        DataInputStream dim = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
	        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
	        Class c = null;
	        Class c2 = null;
	        while(true){
	        	int Int = dim.readInt();
	        	if(Int == 1){
	        		final int buffersize = 4096 * 5;
	    			long size = dim.readLong();
	    			byte[] buf = new byte[buffersize];
	    			File f = new File("class");
	    			DataOutputStream bos = new DataOutputStream(new FileOutputStream(f) );
	    			while(true){
	    				int read; 
	    				read = dim.read(buf,0,min(size,buffersize));
	    				size = size - read;
	    				bos.write(buf,0,read);
	    				if(size <= 0 ){
	    					break;
	    				}
	    			bos.flush();
	    			}
	    			bos.close();
	    			File sendfile = new File("class");
	    			File sendfile2 = new File("class");
	        		FileInputStream fis = new FileInputStream(sendfile);
					FileInputStream fis2 = new FileInputStream(sendfile2);
					FileClassLoader cl =new FileClassLoader(fis);
					FileClassLoader clll =new FileClassLoader(fis2);
				    c = cl.loadClass("mapreduce.test.MyMapp");
				    c2 = clll.loadClass("mapreduce.test.MyMapp");
				    Mapper m = new Mapper();
	        		dos.writeInt(1);
	        		dos.flush();
	        	}
	        	else if(Int == 2){
	        		final int buffersize = 4096 * 5;
	    			long size = dim.readLong();
	    			byte[] buf = new byte[buffersize];
	    			File f = new File("data.txt");
	    			DataOutputStream bos = new DataOutputStream(new FileOutputStream(f) );
	    			while(true){
	    				int read; 
	    				read = dim.read(buf,0,min(size,buffersize));
	    				size = size - read;
	    				bos.write(buf,0,read);
	    				if(size <= 0 ){
	    					break;
	    				}
	    			bos.flush();
	    			}
	    			bos.close();
	        		dos.writeInt(2);
	    			Mapper m = new Mapper();
	    			m.map("data.txt", c, c2);
	        		dos.flush();
	        		dos.writeInt(3);
	        		dos.flush();
	        		File p =new File("data.mpd");
	        		DataPipeThread dpt = new DataPipeThread(p,dos);
	        		dpt.run();
	        	}
	        	else if(Int == 3){
	        		final int buffersize = 4096 * 5;
	    			long size = dim.readLong();
	    			byte[] buf = new byte[buffersize];
	    			File f = new File("mapped.scb");
	    			DataOutputStream bos = new DataOutputStream(new FileOutputStream(f) );
	    			while(true){
	    				int read; 
	    				read = dim.read(buf,0,min(size,buffersize));
	    				size = size - read;
	    				bos.write(buf,0,read);
	    				if(size <= 0 ){
	    					break;
	    				}
	    			bos.flush();
	    			}
	    			bos.close();
	        		dos.writeInt(4);
	    			File sendfile = new File("class");
	        		FileInputStream fis = new FileInputStream(sendfile);
					FileClassLoader cl =new FileClassLoader(fis);
				    c = cl.loadClass("mapreduce.test.MyMapp");
				    Reducer m = new Reducer();
	    			m.reduce("mapped.scb", c);
	    			fis.close();
	        		dos.flush();
	        		dos.writeInt(5);
	        		dos.flush();
	        		File p =new File("mapped.rdc");
	        		DataPipeThread dpt = new DataPipeThread(p,dos);
	        		dpt.run();
	        	}
	        }
	    } catch (IOException | ClassNotFoundException | SecurityException | IllegalArgumentException e) { 
	    	e.printStackTrace(); 
	    } 
	} 
	private static int min(long size,int y) {
		if(size<y)
			return (int) size;
		else
			return y;
	}
}
