package  map_reduce_demo;

import java.net.Socket;
import java.util.*;

public class JobClient {
	public static void main(String[] arg) throws Exception{
		Socket so;
		so = new Socket("localhost",6688);
		while(true){
			System.out.println("Initiating Job...\nInput the job name:");
			Scanner in = new Scanner(System.in);
			String s = in.nextLine();
			System.out.println("Input the mapper's number:");
			int MT = in.nextInt();
			System.out.println("Input the job reducer's number:");
			int RT = in.nextInt();
			System.out.println("Input the job file's path:");
			String path = in.nextLine();
			path = in.nextLine();
			System.out.println("Input the data file's path:");
			String data = in.nextLine();
			int index = path.lastIndexOf("\\");
			String f = path.substring(index + 1);
			index = data.lastIndexOf("\\");
			String d = data.substring(index + 1);
			Job job = new Job(s,f,d,MT,RT);
			JobBack backjob = new JobBack();
			job.classImport(backjob,so);
			System.out.println("Workers Available: "+backjob.getNumber());
			job.fileImport(path,so);
			job.fileImport(data,so);
		}
	}
}
