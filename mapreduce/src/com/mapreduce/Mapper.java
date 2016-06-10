package com.mapreduce;

import java.util.*;

/**
 * MapReduce中Map处理阶段
 * @param <InputKey> Map阶段的输入key类型
 * @param <InputValue> Map阶段输入的value类型
 * @param <OutputKey> Map阶段的输出key类型
 * @param <OutputValue> Map阶段的输出value类型
 */
public abstract class Mapper <InputKey, InputValue, OutputKey, OutputValue> {
	/*
	 *@param ikey 输入key
	 *@param ivalue 输入value
	 *@param okey 输出key
	 *@param ovalue　输出value
	 *
	 * okey和ovalue是同一组键值对，并存储在相同的索引列表中
	*/
	private InputKey ikey;
	private InputValue ivalue;
	private List<OutputKey> okeys;
	private List<OutputValue> ovalues;
		
	protected Mapper(){
		this.okeys = new ArrayList<OutputKey>();
		this.ovalues = new ArrayList<OutputValue>();
	}
	
	Mapper(InputKey key, InputValue value){
		this.ikey = key;
		this.ivalue = value;
		this.okeys = new ArrayList<OutputKey>();
		this.ovalues = new ArrayList<OutputValue>();
	}

	void setKeyValue(InputKey key, InputValue value){
		this.ikey = key;
		this.ivalue = value;
	}

	protected InputKey getInputKey(){
		return  this.ikey;
	}
	

	protected InputValue getInputValue(){
		return  this.ivalue;
	}
	

	List<OutputKey> getKeys(){
		return this.okeys;
	}
	

	List<OutputValue>  getValues(){
		return this.ovalues;
	}
	

	protected void emit(OutputKey okey, OutputValue ovalue){
		this.okeys.add(okey);
		this.ovalues.add(ovalue);
	}
	

	protected abstract void map();
	
}
