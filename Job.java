package map_reduce_demo;

import java.net.Socket; 
import java.io.*;

public class Job implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3130882871721744296L;
	/**
	 * 
	 */
	private String name;
	private String filename;
	private String dataname;
	int MT,RT;
	public Job(String s,String f,String data,int m ,int r){
		this.name = s;
		this.filename=f;
		dataname = data;
		MT = m;
		RT = r;
	}
	public int GetMT(){
		return MT;
	}
	public int GetRT(){
		return RT;
	}
	public String getJobName(){
		return name;
	}
	public String getFileName(){
		return filename;
	}
	public String getDataName(){
		return dataname;
	}
	public void classImport(JobBack backjob,Socket socketConnection) throws Exception{
		Job job =this;
		ObjectOutputStream out = new ObjectOutputStream(socketConnection.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(socketConnection.getInputStream());
		out.writeObject(job);
		out.flush();
		JobBack bjob = (JobBack) in.readObject();
		backjob.setNumber(bjob.getNumber());
	}
	public void fileImport(String path,Socket socketConnection) throws Exception{
		File sendfile = new File(path);
		byte[] buffer = new byte[4086*5];
		if(!sendfile.exists()){
			System.out.println("The file to be sent is not existed!");
			return;
		}
		
		Socket so = socketConnection;
		FileInputStream fis = new FileInputStream(sendfile);
		DataOutputStream dos = new DataOutputStream(so.getOutputStream());
		
		ObjectInputStream in = new ObjectInputStream(socketConnection.getInputStream());
		JobBack bjob = (JobBack) in.readObject();
		bjob.getNumber();
		
		dos.writeUTF("111/#" + sendfile.getName() + "/#" + fis.available());
		dos.flush();
		int size = 0;
		bjob = (JobBack) in.readObject();
		bjob.getNumber();
		while((size = fis.read(buffer))!=-1){
			System.out.println("Data Packet sent with size "+ size);
			dos.write(buffer,0,size);
			dos.flush();
		}
		System.out.println("File transmission finished. Total size: " + fis.available() +" btyes");
		if(fis != null){
			fis.close();
		}
	}
	public void fileReceive(Socket so,ObjectOutputStream out){
		try{
			ObjectOutputStream oos = new ObjectOutputStream(so.getOutputStream());
			JobBack back= new JobBack(0);
			oos.writeObject(back);
			oos.flush();
			DataInputStream dis = new DataInputStream(new BufferedInputStream(so.getInputStream()));
			String coming = dis.readUTF();
			int index = coming.indexOf("/#");
			String code = coming.substring(0,index);
			if(!code.equals("111")){
				System.out.println("Code for file transmission is not right!");
				return;
			}
			coming = coming.substring(index+2);
			index = coming.indexOf("/#");
			String filename = coming.substring(0,index).trim();
			String filesize = coming.substring(index+2).trim();
			oos.writeObject(back);
			oos.flush();
			File file = new File(filename);
			try{
				file.createNewFile();
			}catch(IOException e){
				System.out.println("File Creation Failure!");
			}
			byte[] buffer = new byte[4096*5];
			FileOutputStream fos = null;
			try{
				fos = new FileOutputStream(file);
				long file_size = Long.parseLong(filesize);
				int size = 0;
				long count = 0;
				while(count < file_size){
					size = dis.read(buffer);
					fos.write(buffer, 0, size);
					fos.flush();
					count += size;
					//System.out.println("Get data packet with size "+size);
				}
				System.out.println("Total Size : " + count);
			}catch(FileNotFoundException e){
				System.out.println("Write file Failure!");
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				try{
					if(fos !=null)
						fos.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}catch (IOException e){
			e.printStackTrace();
		}
	}
}

