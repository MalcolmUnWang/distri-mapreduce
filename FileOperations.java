package map_reduce_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class FileOperations {
	public static void separateFile(String file, int num){
		File ffile = new File(file);
		long lon = ffile.length()/((int)num) +1L;
		int index = file.lastIndexOf(".");
		String fname = file.substring(0, index);
		String extend = file.substring(index+1);
		try{
			RandomAccessFile raf1 = new RandomAccessFile(file,"r");
			byte[] bytes = new byte[1024*4];
			byte[] lbyte = new byte[1];
			byte[] encode = {(byte) 0xfe,(byte) 0xff};
			byte[] newline = {0x00,0x0d,0x00,0x0a};
			char key,value,l;
			raf1.read(encode);
			key = raf1.readChar();
			value = raf1.readChar();
			l = raf1.readChar();
			LinkedList<Byte> newline_windows = new LinkedList<Byte>();
			newline_windows.add(newline[0]);
			newline_windows.add(newline[1]);
			newline_windows.add(newline[2]);
			newline_windows.add(newline[3]);
			
			int len = -1;
			for(int i = 0; i < num; i++){
				String name = fname + i + "."+ extend;
				File file2 = new File(name);
				RandomAccessFile raf2  = new RandomAccessFile(file2,"rw");
				raf2.write(encode);
				raf2.writeChar(key);
				raf2.writeChar(value);
				raf2.writeChar(l);
				while((len = raf1.read(bytes)) != -1){
					raf2.write(bytes, 0, len);
					if(raf2.length() > lon){
						LinkedList<Byte> link = new LinkedList<Byte>();
						do{
							if(raf1.read(lbyte)==-1)
								break;
							link.add(lbyte[0]);
							if(link.size()>4)
								link.removeFirst();
							raf2.write(lbyte);
						}while(!link.equals(newline_windows));
						break;
					}
				}
				raf2.close();
			}
			raf1.close();
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	public static boolean mergeFiles(String[] fpaths, String resultPath) {
	    if (fpaths == null || fpaths.length < 1 ) {
	        return false;
	    }
	    if (fpaths.length == 1) {
	        return new File(fpaths[0]).renameTo(new File(resultPath));
	    }

	    File[] files = new File[fpaths.length];
	    for (int i = 0; i < fpaths.length; i ++) {
	        files[i] = new File(fpaths[i]);
	        if ( !files[i].exists() || !files[i].isFile()) {
	            return false;
	        }
	    }

	    File resultFile = new File(resultPath);
	    byte[] k = {(byte) 0xfe,(byte) 0xff};
	    char[] c = new char[3];
	    try {
			RandomAccessFile raf1 = new RandomAccessFile(resultFile,"rw");
			raf1.write(k);
			byte[] bytes = new byte[1024*4];
			for(int i = 0; i < fpaths.length; i++){
				RandomAccessFile raf2 = new RandomAccessFile(files[i],"r");
				if(i != 0){
					raf2.read(k);
					c[0] = raf2.readChar();
					c[1] = raf2.readChar();
					c[2] = raf2.readChar();
				}else{
					raf2.read(k);
				}
				int len;
				while((len = raf2.read(bytes)) != -1){
					raf1.write(bytes,0 , len);
				}
				raf2.close();
				
			}
			raf1.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    /*
	    for (int i = 0; i < fpaths.length; i ++) {
	        files[i].delete();
	    }*/
	    

	    return true;
	}
	
	public static void main(String[] args){
		separateFile("test.txt",10);
		String[] fpath = new String[10];
		for(int i = 0;i<10;i++){
			fpath[i] = "test" + i + ".txt";
		}
		mergeFiles(fpath,"test_merge.txt");
	}
}
