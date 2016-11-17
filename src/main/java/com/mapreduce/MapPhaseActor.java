package com.mapreduce;

/**
 * Created by szp on 2016/11/3.
 */
import WordCount.MapWC;
import WordCount.ReduceWC;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import com.tcp.ServerActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MapPhaseActor extends UntypedActor {
    MapReduce<Integer, String, String, Integer, String, Integer> wcMR =
            new MapReduce<Integer, String, String, Integer, String, Integer>(MapWC.class, ReduceWC.class, "MAP_REDUCE");
    private Logger logger = LogManager.getLogger(MapPhaseActor.class.getName());
    private ActorSelection reduceActor = null;
    private ActorRef serverActor = null;
    public void preStart() throws Exception {
        reduceActor = context().actorSelection("../reduceActor");
    }

    public File[] getFiles(String path){
        File file = new File(path);
        File[] fileList = file.listFiles();
        return fileList;
    }
    public void readFiles(String filename) throws IOException {
        int count=0;
        logger.info("1G 单进程单线程粒度16MMap阶段开始读取文件==================");
        File[] files = getFiles(filename);
        for(File file : files) {
            if (!file.getName().split("\\.")[0].equals("") && file.getName().split("\\.")[0].substring(0, 5).equals("input")){
                RandomAccessFile raf = new RandomAccessFile(new File(file.toString()), "r");
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
                        if(count == 400000){
                            wcMR.startMap();
                            count=0;
                        }
                        count++;
                    }
                }
                fc.close();
                raf.close();
            }
        }
        logger.info("文件全部读取完成");
        wcMR.startMap();
    }
    @Override
    public void onReceive(Object message) throws Throwable {
        if(message instanceof String){
            if("start".equals(message)){
                wcMR.setParallelThreadNum(1);
                readFiles("testData/inputdata");
                wcMR.startShuffle1();
                reduceActor.tell("startReduce",ActorRef.noSender ());

            }else if("fileComplete".equals(message)){
                serverActor.tell("stop",ActorRef.noSender());
                reduceActor.tell("startReduce",ActorRef.noSender ());
                context().stop(getSelf());
            }else if("startTcp".equals(message)){
                serverActor = getContext().actorOf(ServerActor.props(null), "serverActor");
            }
        }

    }
}
