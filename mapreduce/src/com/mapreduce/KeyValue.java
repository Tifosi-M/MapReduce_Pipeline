package com.mapreduce;


public class KeyValue<K extends Comparable<K>, V> implements Comparable<KeyValue<K, V>>{
	K key;
	V value;
	
	KeyValue(K key, V value){
		this.key = key;
		this.value = value;
	}
	
	K getKey(){
		return this.key;
	}
	
	V getValue(){
		return this.value;
	}
	

	@Override
	public int compareTo(KeyValue<K, V> otherkv) {
		// TODO Auto-generated method stub		
		K otherkey = (K) otherkv.getKey();
		return 	this.key.compareTo(otherkey);
	}
}
