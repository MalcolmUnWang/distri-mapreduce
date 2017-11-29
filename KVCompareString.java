package map_reduce_demo;

import java.util.Comparator;

public class KVCompareString implements Comparator<Object>{


	@Override
	public int compare(Object o1, Object o2) {
		if(o1==null && o2 ==null){
			return 0;
		}else if(o1 ==null && o2 != null){
			return -1;
		}else if(o1 != null && o2 == null){
			return 1;
		}
		int i = ((String)((Keyvalue<Object,Object>)o1).key).compareTo((String)((Keyvalue<Object,Object>)o2).key);
		return  (i>0)? 1:((i==0) ? 0:-1);
	}
	
}
