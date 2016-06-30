package com.mapreduce;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by szp on 16/5/27.
 */
public class SpillActor extends UntypedActor {
    private List<KeyValue<String, Integer>> mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
    private int count = 0;
    private static Logger loger = LogManager.getLogger(SpillActor.class.getName());
    private ActorSelection spillMergeActor;
    private volatile int thread_count=0;

    @Override
    public void preStart() throws Exception {
//        spillMergeActoy = getContext().actorOf(Props.create(SpillMergeActor.class), "SpillMergeActor");
        spillMergeActor = getContext().actorSelection("../SpillMergeActor");
    }

    @Override
    public void onReceive(Object message) throws Exception{
        if (message instanceof List) {
            for (KeyValue<String, Integer> item : (List<KeyValue<String, Integer>>) message) {
                mappedKeyValue.add(item);
            }
            ((List<KeyValue<String,Integer>>) message).clear();
            if (mappedKeyValue.size() > 2500000) {
                List<KeyValue<String, Integer>> tmp = mappedKeyValue;
                mappedKeyValue  = null;
                mappedKeyValue = new LinkedList<>();
                new Thread(()->{
                    registThread();
                    int tmpcount = count++;
//                    Collections.sort(tmp);
                    loger.debug("正在写入文件" + tmpcount);
                    File srcFile = new File("/root/spill_out/" + tmpcount + ".txt");
                    RandomAccessFile raf = null;
                    try {
                        raf = new RandomAccessFile(srcFile, "rw");
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    FileChannel fileChannel = raf.getChannel();
                    ByteBuffer rBuffer = ByteBuffer.allocateDirect(32 * 1024 * 1024);
                    try {
                        int size = tmp.size();
                        for (int i = 0; i < size; i++) {
                            KeyValue<String, Integer> keyValue = tmp.remove(0);
//                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                            rBuffer.put((keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n").getBytes());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    rBuffer.flip();
                    try {
                        fileChannel.write(rBuffer);
                        fileChannel.close();
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    unregistThread();
                    loger.debug("文件" + tmpcount+"写入结束");
                }).start();

            }
//            if (count == 2) {
//                spillMergeActor.tell("StartMerge", getSelf());
//            }
//            if (count > 2) {
//                spillMergeActor.tell("Merge", getSelf());
//            }

        }
        if (message instanceof String) {
            if ("END".equals((String) message)) {
                new Thread(()->{
                    registThread();
//                    Collections.sort(mappedKeyValue);
                    loger.debug("正在写入文件" + count);
                    File srcFile = new File("/root/spill_out/" + count + ".txt");
                    RandomAccessFile raf = null;
                    try {
                        raf = new RandomAccessFile(srcFile, "rw");
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    FileChannel fileChannel = raf.getChannel();
                    ByteBuffer rBuffer = ByteBuffer.allocate(32 * 1024 * 1024);
                    try {
                        int size = mappedKeyValue.size();
                        for (int i = 0; i < size; i++) {
                            KeyValue<String, Integer> keyValue = mappedKeyValue.remove(0);
//                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                            rBuffer.put((keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n").getBytes());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    rBuffer.flip();
                    try {
                        fileChannel.write(rBuffer);
                        fileChannel.close();
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    mappedKeyValue = null;
                    loger.debug("文件" + count+"写入结束");
                    count++;
//                if (count == 2) {
//                    spillMergeActor.tell("StartMerge", getSelf());
//                }
//                if (count > 2) {
//                    spillMergeActor.tell("Merge", getSelf());
//                }
                    unregistThread();
                }).start();
                while(thread_count!=0);
                loger.info("溢写完成");
                spillMergeActor.tell("StartMerge", getSelf());
                context().stop(getSelf());
            }
        }
    }
    public void registThread(){
        synchronized(this){
            this.thread_count++;
        }
    }
    public void unregistThread(){
        synchronized(this){
            this.thread_count--;
        }
    }

}
