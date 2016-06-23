package WordCount;

import com.mapreduce.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Main {
	MapReduce<Integer, String, String, Integer, String, Integer> wcMR =
			new MapReduce<Integer, String, String, Integer, String, Integer>(MapWC.class, ReduceWC.class, "MAP_REDUCE");
	private static Logger logger = LogManager.getLogger(Main.class.getName());

	public void init(){
		wcMR.setParallelThreadNum(1);

		try {
			readFile("/root/input.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}

		wcMR.startShuffle();
//		wcMR.startReduce();
//		wcMR.writeToFile();

	}

	public void readFile(String filename) throws IOException {
		int count=0;
		logger.info("开始读取文件==================");
		RandomAccessFile raf = new RandomAccessFile(new File(filename), "r");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer mbb =  fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
		StringBuffer sbf = new StringBuffer();
		while(mbb.remaining()>0){
			char data = (char)mbb.get();
			if(data!='\n'){
				sbf.append(data);
			}else{
				wcMR.addKeyValue(0,sbf.toString());
				sbf.setLength(0);
				if(count == 800000){
					wcMR.startMap();
					count=0;
				}
				count++;
			}
		}
		fc.close();
		raf.close();
//		LineIterator it = FileUtils.lineIterator(new File(filename), "UTF-8");
//		try {
//			while (it.hasNext()) {
//				String line = it.nextLine();
//				wcMR.addKeyValue(0, line);
//				if (count == 800000) {
//					wcMR.startMap();
//					count = 0;
//				}
//				count++;
//			}
//		} finally {
//			LineIterator.closeQuietly(it);
//		}
		logger.info("文件全部读取完成");
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
