package WordCount;

import com.mapreduce.*;
import java.io.*;

public class Main {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String filename="1.txt";
			
		MapReduce<Integer, String, String, Integer, String, Integer> wcMR =
				new MapReduce<Integer, String, String, Integer, String, Integer>(MapWC.class, ReduceWC.class, "MAP_REDUCE");
		wcMR.setParallelThreadNum(6);

		try{
			FileReader file = new FileReader(filename);
			BufferedReader buffer = new BufferedReader(file);
			String s;
			while((s = buffer.readLine())!=null){
				wcMR.addKeyValue(0 , s);
			}
		}catch(Exception e){
			System.err.println("文件读取失败");
	    }
		
		wcMR.run();				
	}

}
