package WordCount;

import com.mapreduce.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class Main {
	MapReduce<Integer, String, String, Integer, String, Integer> wcMR =
			new MapReduce<Integer, String, String, Integer, String, Integer>(MapWC.class, ReduceWC.class, "MAP_REDUCE");
	private static Logger logger = LogManager.getLogger(Main.class.getName());

	public void init(){
		wcMR.setParallelThreadNum(4);

		try {
			readFile("/Users/szp/Documents/github/MapReduce_Pipeline/mapreduce/2.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.debug("读取文件结束");

		wcMR.startShuffle();
		wcMR.startReduce();
		wcMR.writeToFile();

	}
	public void readFile(String filename) throws IOException {
		int count=0;
		logger.debug("开始读取文件");
//		try{
//			FileReader file = new FileReader(filename);
//			BufferedReader buffer = new BufferedReader(file);
//			String s;
//			while((s = buffer.readLine())!=null){
//				wcMR.addKeyValue(0 , s);
//			}
//			buffer.close();
//		}catch(Exception e){
//			System.err.println("文件读取失败");
//			e.printStackTrace();
//		}

		LineIterator it = FileUtils.lineIterator(new File(filename), "UTF-8");
		try {
			while (it.hasNext()) {
				String line = it.nextLine();
				wcMR.addKeyValue(0, line);
				if (count == 800000) {
					wcMR.startMap();
					count = 0;
				}
				count++;
			}
		} finally {
			LineIterator.closeQuietly(it);
		}
		wcMR.startMap();

	}
	public void run(){
		wcMR.run();
	}

	public static void main(String[] args) {
		Main main = new Main();
		main.init();
	}


}
