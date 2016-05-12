package com.mapreduce;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Reduce输出数据类
 * @param <K> Reduce输出key类型
 * @param <V> Reduce输出value类型
 */
public class OutputData<K, V> {
	List<K> keys;
	List<V> values;
	
	OutputData(){
		this.keys = new ArrayList<K>();
		this.values = new ArrayList<V>();
	}
	
	void setKeyValue(K key, V value){
		this.keys.add(key);
		this.values.add(value);
	}
	
	void reduceShow(){
		for(int i = 0; i < keys.size(); i++){
			System.out.println(keys.get(i).toString() + ", " + values.get(i).toString());
		}
	}
	
	List<K> getOutputKeys(){
		return this.keys;
	}
	
	List<V> getOutputValues(){
		return this.values;
	}
	
	
}
