package com.mapreduce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
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
	private static Logger logger = LogManager.getLogger(OutputData.class.getName());
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
	//运行完成，写出结果到文本文件
	public void writeToFile() {
		logger.debug("结果写入文件");
		File file=new File("/root/spill_out/result.txt");
		if(!file.exists())
			try {
				file.createNewFile();
				FileOutputStream out=new FileOutputStream(file,true);
				for(int i=0;i<keys.size();i++){
					StringBuffer sb=new StringBuffer();
					sb.append(keys.get(i).toString() + ", " + values.get(i).toString()+"\n");
					out.write(sb.toString().getBytes("utf-8"));
				}
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

	}
	
	List<K> getOutputKeys(){
		return this.keys;
	}
	
	List<V> getOutputValues(){
		return this.values;
	}


}
