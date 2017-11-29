package map_reduce_demo;

import java.io.IOException;
import java.net.Socket;

public class JobClientState extends ClientState{
	public JobClientState(Socket so) throws IOException {
		super(so);
	}
	
}
