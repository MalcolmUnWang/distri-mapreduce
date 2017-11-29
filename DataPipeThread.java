package map_reduce_demo;
import java.io.*;
import java.net.*;

public class DataPipeThread{
	DataInputStream Dis;
	DataOutputStream Dos;
	long size;
	public DataPipeThread(File file,DataOutputStream dos) throws IOException{
		size = file.length();
		Dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
		Dos = dos;
	}
	public void run(){
		try{
			final int buffersize = 4086*5;
			Dos.writeLong(size);
			byte[] buf = new byte[buffersize];
			while(true){
				int read = 0;
				if( (read = Dis.read(buf, 0, min(size,buffersize)) ) == -1 ){
					break;
				}
				Dos.write(buf,0,read);
				Dos.flush();
			}
			Dis.close();
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
