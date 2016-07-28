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
import java.util.ListIterator;

/**
 * Created by szp on 16/7/27.
 */
public class SpillActor2 extends UntypedActor {
    private List<KeyValue<String, Integer>> mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
//    private int count = 0;
    private static Logger loger = LogManager.getLogger(SpillActor2.class.getName());
    private volatile int thread_count = 0;

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof List) {
            loger.info("溢写阶段接受到数据-------");
            for (KeyValue<String, Integer> item : (List<KeyValue<String, Integer>>) message) {
                mappedKeyValue.add(item);
            }
            ((List<KeyValue<String, Integer>>) message).clear();
            if (mappedKeyValue.size() > 5000000) {
                loger.info("数据达到5000000准备进行溢写");
                List<KeyValue<String, Integer>> tmp = mappedKeyValue;
                mappedKeyValue = null;
                mappedKeyValue = new LinkedList<>();
                new Thread(() -> {
                    registThread();
                    int tmpcount = StaticCount.count++;
//                    Collections.sort(tmp);
                    KeyValue[] tmps = tmp.toArray(new KeyValue[0]);
                    QuickSort<KeyValue<String, Integer>> quickSort = new QuickSort<KeyValue<String, Integer>>(tmps);
                    quickSort.sort();
                    ListIterator<KeyValue<String, Integer>> it = tmp.listIterator();
                    for (KeyValue<String, Integer> e : tmps) {
                        it.next();
                        it.set(e);
                    }
                    loger.debug("正在写入文件" + tmpcount);
                    File srcFile = new File("testData/spill_out/" + tmpcount + ".txt");
                    RandomAccessFile raf = null;
                    try {
                        raf = new RandomAccessFile(srcFile, "rw");
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    FileChannel fileChannel = raf.getChannel();
                    ByteBuffer rBuffer = ByteBuffer.allocateDirect(128 * 1024 * 1024);
                    try {
                        int size = tmp.size();
                        for (int i = 0; i < size; i++) {
                            KeyValue<String, Integer> keyValue = tmp.remove(0);
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
                    loger.debug("文件" + tmpcount + "写入结束");
                }).start();
            }

        }

    }

    public void registThread() {
        synchronized (this) {
            this.thread_count++;
        }
    }

    public void unregistThread() {
        synchronized (this) {
            this.thread_count--;
        }
    }
}
