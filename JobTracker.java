package map_reduce_demo;
import java.util.concurrent.LinkedBlockingQueue;

public class JobTracker {
	public static void main(String arg[]){
		LinkedBlockingQueue<Job> jobqueue;
		WorkState ws;
		jobqueue = new LinkedBlockingQueue<Job>();
		ws = new WorkState();
		JobServerThread jst = new JobServerThread(jobqueue,ws);
		jst.start();
		TaskServerThread tst = new TaskServerThread(jobqueue,ws);
		tst.start();
	}
}
