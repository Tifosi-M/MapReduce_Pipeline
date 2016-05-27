package com.mapreduce;

/*
 */
/**
 *
 * MapReduce的Reduce阶段
 * @param <InputKey> Reduce输入key类型
 * @param <InputValue> Reduce输入value类型
 * @param <OutputKey> Reduce输出key类型
 * @param <InputKey> Reduce输出value类型
 */
public abstract class Reducer<InputKey, InputValue, OutputKey, OutputValue>{
	/**
	 *@param ikey 输入key
	 *@param ivalues 输入value
	 *@param okey 输出key
	 *@param ovalue　输出value
	 *
	*/
	InputKey ikey;
	GroupedValues<InputValue> ivalues;
	OutputKey okey;
	OutputValue ovalue;
	
	protected Reducer(){
	}

	void setKeyValue(InputKey key, GroupedValues<InputValue> groupedInputValues){
		this.ikey = key;
		this.ivalues = groupedInputValues;
	}	

	protected InputKey getInputKey(){
		return this.ikey;
	}

	protected GroupedValues<InputValue> getInputValue(){
		return this.ivalues;
	}


	OutputKey getKey(){
		return okey;
	}
	

	OutputValue getValue(){
		return ovalue;
	}

	protected void emit(OutputKey key, OutputValue value){
		this.okey = key;
		this.ovalue = value;
	}

	protected abstract void reduce();
	
}
