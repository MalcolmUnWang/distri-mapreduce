package map_reduce_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class Reducer {
	public int serverCombine(String filename, Class c){
		int index = filename.lastIndexOf(".");
		String fname = filename.substring(0, index);
		int i = 0;
		try{
			RandomAccessFile raf1 = new RandomAccessFile(filename,"r");
			File ffname = new File(fname + "." + "scb");
			RandomAccessFile raf2 = new RandomAccessFile(ffname,"rw");
			byte[] word = new byte[1];
			byte[] encode = new byte[2];
			byte[] newline = {0x00,0x23};
			LinkedList<Byte> newline_windows = new LinkedList<Byte>();
			
			ArrayList<Keyvalue<Object,Object>> mapped = new ArrayList<Keyvalue<Object,Object>>();

			newline_windows.add(newline[0]);
			newline_windows.add(newline[1]);
			raf1.read(encode);
			raf2.write(encode);
			char key,value,l;
			key = raf1.readChar();
			value = raf1.readChar();
			l = raf1.readChar();
			Class keyc = null,valuec =null;
			if(key == '\u0030'){
				keyc = int.class;
			}else if(key == '\u0031'){
				keyc = byte[].class;
			}else if(key == '\u0032'){
				keyc = float.class;
			}else if(key == '\u0033'){
				keyc = double.class;
			}else if(key == '\u0034'){
				keyc = String.class;
			}
			if(value == '\u0030'){
				valuec = int.class;
			}else if(value == '\u0031'){
				valuec = byte[].class;
			}else if(value == '\u0032'){
				valuec = float.class;
			}else if(value == '\u0033'){
				valuec = double.class;
			}else if(value == '\u0034'){
				valuec = String.class;
			}
			
			LinkedList<Byte> link = new LinkedList<Byte>();
			ArrayList<Object> readarray = new ArrayList<Object>();
			while(raf1.read(word)!=-1){
				if(readarray.size()==2902){
					int ppp = 1;
				}
				
				byte[] bytearray = new byte[1024*30];
				int n = 0;
				Arrays.fill(bytearray,n,n+1,word[0]);
				link.add(word[0]);
				if(link.size()>2)
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
					if(link.size()>2)
						link.removeFirst();
				}while(!link.equals(newline_windows));
				if(n>1024*20){
					System.out.println("File format is not correct! The size of one value has overflowed!");
					return -1;
				}
				byte[] trun = Arrays.copyOf(bytearray,n-2);
				ArrayList<Object> va = new ArrayList<Object>();
				if(valuec == int.class){
					do{
						int p = raf1.readInt();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}else if(valuec == float.class){
					do{
						float p = raf1.readFloat();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}else if(valuec == double.class){
					do{
						double p = raf1.readDouble();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}
				raf1.readChar();
				raf1.readChar();
				
				Keyvalue<Object,Object> kv = new Keyvalue<Object,Object>(trun,va);
				readarray.add((Object)kv);
			}
			
			Object[] map_array = readarray.toArray();
			ArrayList<Object> conbinekey = new ArrayList<Object>();
			ArrayList<ArrayList<Object>> conbinevalue = new ArrayList<ArrayList<Object>>();	
			
			if(keyc == int.class){
				KVCompareInt kvint = new KVCompareInt();
				Arrays.parallelSort(map_array, kvint);
				Object last = null;
				for(Object e : map_array){
					if(kvint.compare(last, e) == 0){
						ArrayList<Object> value1 = conbinevalue.get(conbinevalue.size()-1);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
					}else{
						ArrayList<Object> value1 = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
						conbinevalue.add(value1);
						last = e;
					}
				}
			}else if(keyc == byte[].class){
				KVComparebyte kvbyte = new KVComparebyte();
				Arrays.parallelSort(map_array,kvbyte);
				Object last = null;
				for(Object e : map_array){
					if(kvbyte.compare(last, e) == 0){
						ArrayList<Object> value1 = conbinevalue.get(conbinevalue.size()-1);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
					}else{
						ArrayList<Object> value1 = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
						conbinevalue.add(value1);
						last = e;
					}
				}
			}else if(keyc == String.class){
				KVCompareString kvstring = new KVCompareString();
				Arrays.parallelSort(map_array,kvstring);
				Object last = null;
				for(Object e : map_array){
					if(kvstring.compare(last, e) == 0){
						ArrayList<Object> value1 = conbinevalue.get(conbinevalue.size()-1);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
					}else{
						ArrayList<Object> value1 = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
						conbinevalue.add(value1);
						last = e;
					}
				}
			}else if(keyc == float.class){
				KVCompareFloat kvfloat = new KVCompareFloat();
				Arrays.parallelSort(map_array,kvfloat);
				Object last = null;
				for(Object e : map_array){
					if(kvfloat.compare(last, e) == 0){
						ArrayList<Object> value1 = conbinevalue.get(conbinevalue.size()-1);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
					}else{
						ArrayList<Object> value1 = new ArrayList<Object>();
						conbinekey.add(((Keyvalue<Object,Object>)e).key);
						value1.addAll((ArrayList<Object>)(((Keyvalue<Object,Object>)e).value));
						conbinevalue.add(value1);
						last = e;
					}
				}
			}else{
				System.out.println("Class is illegal!");
				return -1;
			}
			
			if(c != null){
				//ArrayList<byte[]> a = new ArrayList<byte[]>();
	        	Object obj = c.newInstance();
	        	Class[] ll = {ArrayList.class,ArrayList.class,RandomAccessFile.class};
		        Method method = c.getDeclaredMethod("conbiner",ll);
		        method.invoke(obj,conbinekey,conbinevalue,raf2);
            }
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
	
	public int reduce(String filename, Class c){
		int index = filename.lastIndexOf(".");
		String fname = filename.substring(0, index);
		int i = 0;
		System.out.println("Reducing...");
		try{
			RandomAccessFile raf1 = new RandomAccessFile(filename,"r");
			File ffname = new File(fname + "." + "rdc");
			RandomAccessFile raf2 = new RandomAccessFile(ffname,"rw");
			byte[] word = new byte[1];
			byte[] encode = new byte[2];
			byte[] newline = {0x00,0x23};
			LinkedList<Byte> newline_windows = new LinkedList<Byte>();
			

			newline_windows.add(newline[0]);
			newline_windows.add(newline[1]);
			raf1.read(encode);
			raf2.write(encode);
			char key,value,l;
			key = raf1.readChar();
			value = raf1.readChar();
			l = raf1.readChar();
			Class keyc = null,valuec =null;
			if(key == '\u0030'){
				keyc = int.class;
			}else if(key == '\u0031'){
				keyc = byte[].class;
			}else if(key == '\u0032'){
				keyc = float.class;
			}else if(key == '\u0033'){
				keyc = double.class;
			}else if(key == '\u0034'){
				keyc = String.class;
			}
			if(value == '\u0030'){
				valuec = int.class;
			}else if(value == '\u0031'){
				valuec = byte[].class;
			}else if(value == '\u0032'){
				valuec = float.class;
			}else if(value == '\u0033'){
				valuec = double.class;
			}else if(value == '\u0034'){
				valuec = String.class;
			}
			
			LinkedList<Byte> link = new LinkedList<Byte>();
			while(raf1.read(word)!=-1){
				
				byte[] bytearray = new byte[1024*30];
				int n = 0;
				Arrays.fill(bytearray,n,n+1,word[0]);
				link.add(word[0]);
				if(link.size()>2)
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
					if(link.size()>2)
						link.removeFirst();
				}while(!link.equals(newline_windows));
				if(n>1024*30){
					System.out.println("File format is not correct! The size of one value has overflowed!");
					return -1;
				}
				byte[] trun = Arrays.copyOf(bytearray,n-2);
				ArrayList<Object> va = new ArrayList<Object>();
				if(valuec == int.class){
					do{
						int p = raf1.readInt();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}else if(valuec == float.class){
					do{
						float p = raf1.readFloat();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}else if(valuec == double.class){
					do{
						double p = raf1.readDouble();
						va.add((Object)p);
					}while(raf1.readChar() == '\u003b');
				}
				raf1.readChar();
				raf1.readChar();
				
				Keyvalue<Object,Object> kv = new Keyvalue<Object,Object>(trun,va);
				Object kkey = (Object)(kv.key);
				ArrayList<Object> vvalue = (ArrayList<Object>)(kv.value);
				
				if(c != null){
					//ArrayList<byte[]> a = new ArrayList<byte[]>();
		        	Object obj = c.newInstance();
		        	Class[] ll = {Object.class,ArrayList.class,RandomAccessFile.class};
			        Method method = c.getDeclaredMethod("reducer",ll);
			        method.invoke(obj,kkey,vvalue,raf2);
	            }
			}
			
			
			
			System.out.println("Reducing has been done!");
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
	
	public static void main(String[] argc){
		System.out.println("The Server is combining...");
		File sendfile = new File("D:\\Files built by Me\\Java\\Welcome\\bin\\mapreduce\\test\\MyMapp.class");
		File sendfile1 = new File("D:\\Files built by Me\\Java\\Welcome\\bin\\mapreduce\\test\\MyMapp.class");
		try {
			FileInputStream fis = new FileInputStream(sendfile);
			FileClassLoader cl =new FileClassLoader(fis);
		    Class c = cl.loadClass("mapreduce.test.MyMapp");
		    Reducer m = new Reducer();
	        m.serverCombine("bible_merge.txt", c);
	        fis.close();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Server Combining has been done!\n Reducing...");
		try {
			FileInputStream fis = new FileInputStream(sendfile1);
			FileClassLoader cl =new FileClassLoader(fis);
		    Class c = cl.loadClass("mapreduce.test.MyMapp");
		    Reducer m = new Reducer();
	        m.reduce("bible_merge.scb", c);
	        fis.close();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Reducing has been done!");
	}
}
