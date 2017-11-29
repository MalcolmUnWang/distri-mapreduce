package map_reduce_demo;

import java.util.Comparator;

public class KVCompareInt implements Comparator<Object>{


	@Override
	public int compare(Object o1, Object o2) {
		if(o1==null && o2 ==null){
			return 0;
		}else if(o1 ==null && o2 != null){
			return -1;
		}else if(o1 != null && o2 == null){
			return 1;
		}
		if((int)((Keyvalue<Object,Object>)o1).key < (int)((Keyvalue<Object,Object>)o2).key){
			return -1;
		}else if((int)((Keyvalue<Object,Object>)o1).key == (int)((Keyvalue<Object,Object>)o2).key){
			return 0;
		}else{
			return 1;
		}
	}
	
}

