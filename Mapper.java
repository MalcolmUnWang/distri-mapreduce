package map_reduce_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Arrays;

public class Mapper{
	
	public boolean check(String s){
		return true;
	}
	public boolean check(float s){
		return false;
	}
	public boolean check(double s){
		return false;
	}
	public boolean check(long s){
		return false;
	}
	public boolean check(int s){
		return false;
	}
	public  int map(String filename,Class c,Class c2){
		int index = filename.lastIndexOf(".");
		String fname = filename.substring(0, index);
		int i = 0;
		try{
			RandomAccessFile raf1 = new RandomAccessFile(filename,"r");
			File ffname = new File(fname + "." + "mpd");
			RandomAccessFile raf2 = new RandomAccessFile(ffname,"rw");
			byte[] word = new byte[1];
			byte[] encode = new byte[2];
			byte[] newline = {0x00,0x0d,0x00,0x0a};
			LinkedList<Byte> newline_windows = new LinkedList<Byte>();
			
			ArrayList<Keyvalue<Object,Object>> mapped = new ArrayList<Keyvalue<Object,Object>>();
			
			newline_windows.add(newline[0]);
			newline_windows.add(newline[1]);
			newline_windows.add(newline[2]);
			newline_windows.add(newline[3]);
			raf1.read(encode);
			raf2.write(encode);
			LinkedList<Byte> link = new LinkedList<Byte>();
			/*
			Object[] on = new Object[1];
			if(c2 != null){
	        	Object obj1 = c2.newInstance();
	        	Class[] l1 = {Object[].class,RandomAccessFile.class};
		        Method method1 = c2.getDeclaredMethod("conbiner",l1);
		        method1.invoke(obj1,on,raf2);
            }*/
			System.out.println("Mapping...");
			while(raf1.read(word)!=-1){
				byte[] bytearray = new byte[1024*4];
				int n = 0;
				Arrays.fill(bytearray,n,n+1,word[0]);
				link.add(word[0]);
				if(link.size()>4)
					link.removeFirst();
				n++;
				i++;
				do{
					if(raf1.read(word)==-1){
						break;
					}
					Arrays.fill(bytearray,n,n+1,word[0]);
					link.add(word[0]);
					n++;
					i++;
					if(link.size()>4)
						link.removeFirst();
				}while(!link.equals(newline_windows));
				if(n>1024*4){
					System.out.println("File format is not correct! The size of one value has overflowed!");
					return -1;
				}
				byte[] trun = Arrays.copyOf(bytearray,n-4);
				int[] size = new int[1];
				Object[] mappedk = new Object[1024];
				Object[] mappedv = new Object[1024];
				size[0] = 0;
				if(c != null){
					//ArrayList<byte[]> a = new ArrayList<byte[]>();
		        	Object obj = c.newInstance();
		        	Class[] l = {int.class,byte[].class,Object[].class,Object[].class,int[].class};
			        Method method = c.getDeclaredMethod("mapper",l);
			        method.invoke(obj,i,trun,mappedk,mappedv,size);
	            }
				for(int k = 0; k < size[0]; k++){
					Keyvalue<Object,Object> e = new Keyvalue<Object,Object>(mappedk[k],mappedv[k]);
					mapped.add(e);
				}
			}
			System.out.println("Mapping has been done!\nCombining...");
			Object[] map_array = mapped.toArray();
			//System.out.println(map_array[0].getClass().getName());
			//System.out.println(((Keyvalue<Object,Object>)map_array[0]).key.getClass().getName() == ((Keyvalue<Object,Object>)map_array[0]).value.getClass().getName());
			//Keyvalue<Object,Object>[] map_arrays = (Keyvalue<Object,Object>[]) map_array;
			Class<?> cl = ((Keyvalue<Object,Object>)map_array[0]).key.getClass();
			ArrayList<Object> conbinekey = new ArrayList<Object>();
			ArrayList<ArrayList<Object>> conbinevalue = new ArrayList<ArrayList<Object>>();	
			
			if(cl == int.class){
				KVCompareInt kvint = new KVCompareInt();
				Arrays.parallelSort(map_array, kvint);
				Object last = null;
				for(Object e : map_array){
					if(kvint.compare(last, e) == 0){
						ArrayList<Object> value = conbinevalue.get(conbinevalue.size()-1);
						value.add(((Keyvalue<Object,Object>)e).value);
					}else{
						ArrayList<Object> value = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value.add(((Keyvalue<Object,Object>)e).value);
						conbinevalue.add(value);
						last = e;
					}
				}
			}else if(cl == byte[].class){
				KVComparebyte kvbyte = new KVComparebyte();
				Arrays.parallelSort(map_array,kvbyte);
				Object last = null;
				for(Object e : map_array){
					if(kvbyte.compare(last, e) == 0){
						ArrayList<Object> value = conbinevalue.get(conbinevalue.size()-1);
						value.add(((Keyvalue<Object,Object>)e).value);
					}else{
						ArrayList<Object> value = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value.add(((Keyvalue<Object,Object>)e).value);
						conbinevalue.add(value);
						last = e;
					}
				}
			}else if(cl == String.class){
				KVCompareString kvstring = new KVCompareString();
				Arrays.parallelSort(map_array,kvstring);
				Object last = null;
				for(Object e : map_array){
					if(kvstring.compare(last, e) == 0){
						ArrayList<Object> value = conbinevalue.get(conbinevalue.size()-1);
						value.add(((Keyvalue<Object,Object>)e).value);
					}else{
						ArrayList<Object> value = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value.add(((Keyvalue<Object,Object>)e).value);
						conbinevalue.add(value);
						last = e;
					}
				}
			}else if(cl == float.class){
				KVCompareFloat kvfloat = new KVCompareFloat();
				Arrays.parallelSort(map_array,kvfloat);
				Object last = null;
				for(Object e : map_array){
					if(kvfloat.compare(last, e)==0){
						ArrayList<Object> value = conbinevalue.get(conbinevalue.size()-1);
						value.add(((Keyvalue<Object,Object>)e).value);
					}else{
						ArrayList<Object> value = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value.add(((Keyvalue<Object,Object>)e).value);
						conbinevalue.add(value);
						last = e;
					}
				}
			}else{
				System.out.println("Class is illegal!");
				return -1;
			}
			System.out.println("Combining has been done!\nWriting...");
			
			if(c2 != null){
				//ArrayList<byte[]> a = new ArrayList<byte[]>();
	        	Object obj = c2.newInstance();
	        	Class[] l = {ArrayList.class,ArrayList.class,RandomAccessFile.class};
		        Method method = c2.getDeclaredMethod("conbiner",l);
		        method.invoke(obj,conbinekey,conbinevalue,raf2);
            }
			System.out.println("Writing has been done!\n");
			
			raf1.close();
			raf2.close();
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public static void main(String[] args){
		FileOperations.separateFile("Bible.txt",10);
		String[] fpath = new String[10];
		for(int i = 0;i<10;i++){
			fpath[i] = "Bible" + i + ".mpd";
			File sendfile = new File("D:\\Files built by Me\\Java\\Welcome\\bin\\mapreduce\\test\\MyMapp.class");
			File sendfile2 = new File("D:\\Files built by Me\\Java\\Welcome\\bin\\mapreduce\\test\\MyMapp.class");
			try {
				FileInputStream fis = new FileInputStream(sendfile);
				FileInputStream fis2 = new FileInputStream(sendfile2);
				FileClassLoader cl =new FileClassLoader(fis);
				FileClassLoader clll =new FileClassLoader(fis2);
			    Class c = cl.loadClass("mapreduce.test.MyMapp");
			    Class c2 = clll.loadClass("mapreduce.test.MyMapp");
			    Mapper m = new Mapper();
		        m.map( "Bible" + i + ".txt", c,c2);
		        fis.close();
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		FileOperations.mergeFiles(fpath,"bible_merge.txt");
		System.out.println("Merged!");
	}
}
