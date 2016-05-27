package WordCount;

import com.mapreduce.*;

public class ReduceWC extends Reducer<String, Integer, String, Integer>{

	protected void reduce(){
		int sum = 0;
		for(Integer num : this.getInputValue()){
			sum++;
		}
		emit(this.getInputKey(), sum);
	}
}
