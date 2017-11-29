package map_reduce_demo;

import java.util.Comparator;

public class KVComparebyte implements Comparator<Object>{


	@Override
	public int compare(Object o1, Object o2) {
		if(o1==null && o2 ==null){
			return 0;
		}else if(o1 ==null && o2 != null){
			return -1;
		}else if(o1 != null && o2 == null){
			return 1;
		}
		for(int i = 0; i<((byte[])((Keyvalue<Object,Object>)o1).key).length && i<((byte[])((Keyvalue<Object,Object>)o2).key).length;i++){
			if(((byte[])((Keyvalue<Object,Object>)o1).key)[i]<((byte[])((Keyvalue<Object,Object>)o2).key)[i]){
				return -1;
			}else if(((byte[])((Keyvalue<Object,Object>)o1).key)[i] > ((byte[])((Keyvalue<Object,Object>)o2).key)[i]){
				return 1;
			}
		}
		if(((byte[])((Keyvalue<Object,Object>)o1).key).length<((byte[])((Keyvalue<Object,Object>)o2).key).length){
			return -1;
		}else if (((byte[])((Keyvalue<Object,Object>)o1).key).length == ((byte[])((Keyvalue<Object,Object>)o2).key).length){
			return 0;
		}else{
			return 1;
		}
	}
	
}