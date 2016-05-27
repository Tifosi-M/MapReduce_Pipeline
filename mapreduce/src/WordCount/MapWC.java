package WordCount;

import java.util.*;

import com.mapreduce.*;

public class MapWC extends Mapper<Integer, String, String, Integer>{

	protected void map(){
		String line = this.getInputValue();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()){
			emit(tokenizer.nextToken(), new Integer(1));
		}
	}
}




