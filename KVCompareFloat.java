package map_reduce_demo;

import java.util.Comparator;

public class KVCompareFloat implements Comparator<Object>{


	@Override
	public int compare(Object o1, Object o2) {
		if((float)((Keyvalue<Object,Object>)o1).key < (float)((Keyvalue<Object,Object>)o2).key){
			return -1;
		}else if((float)((Keyvalue<Object,Object>)o1).key == (float)((Keyvalue<Object,Object>)o2).key){
			return 0;
		}else{
			return 1;
		}
	}
	
}
