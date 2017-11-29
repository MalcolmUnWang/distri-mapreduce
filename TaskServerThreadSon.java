package map_reduce_demo;
import java.net.*;
import java.io.*;

public class TaskServerThreadSon extends Thread{
	Socket socket; 
	public TaskServerThreadSon(Socket so){ 
		socket = so;
	} 
	public void run(){ 
		try {
			System.out.println("A new taskclient has been connected");
			ObjectInputStream ois =new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Signal s=null;
		Object obj;
	}
}

