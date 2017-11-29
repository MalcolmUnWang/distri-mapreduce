package map_reduce_demo;


public class  Keyvalue <K,V>  {
	K key;
	V value;
	public Keyvalue(){
		
	}
	public Keyvalue(K k,V v){
		key = k;
		value = v;
	}
	
}
/*
public interface Keyvalue<K,V> extends Comparable<Keyvalue<K,V>>{
	int compareTo(Keyvalue<K, V> o);
}*/
