package com.mapreduce;

import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/7/20.
 */
public class MultiMain {
    private static Logger logger = LogManager.getLogger(MultiMain.class.getName());
    public static void main(String[] args) throws IOException {
        logger.info("程序开始================");

        File src = new File("testData/inputdata");
        File[] files = src.listFiles((FilenameFilter) new SuffixFileFilter("txt"));
        ProcessBuilder builder;
        boolean flag = false;
        List<Process> list_process = new ArrayList<Process>();
        int count = 0;
        List<ProcessBuilder> list_builder = new ArrayList<ProcessBuilder>();
        for (File file : files) {
            list_builder.add(new ProcessBuilder("java", "-jar", "MapReduce.jar", file.toString(),String.valueOf(count++)));
        }

        while (list_builder.size() > 0) {
            if (list_process.size() >= 3) {
                while(true){
                    for(Process p : list_process){
                        if(!p.isAlive()){
                            list_process.remove(p);
                            flag = true;
                            break;
                        }
                    }
                    if(flag == true){
                        flag = false;
                        break;
                    }
                }
            }
            list_process.add(list_builder.remove(0).start());

        }

//        try {
//            String[] cmds = {"java", "-jar", "test.jar", "testData/inputdata/input.txt"};
//            builder = new ProcessBuilder(cmds);
//
//            //合并输出流和错误流
//            builder.redirectErrorStream(true);
//
//            //启动进程
//            Process process = builder.start();
//
//            //获得输出流
//            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), "utf-8"));
//            String line = null;
//            while (null != (line = br.readLine())) {
//                System.out.println(line);
//            }
//            process.destroy();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
