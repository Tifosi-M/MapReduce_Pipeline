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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.util.*;

/**
 * Created by szp on 16/5/27.
 */
public class SpillActor extends UntypedActor {
    private List<KeyValue<String, Integer>> mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
    //    private int count = 0;
    private static Logger loger = LogManager.getLogger(SpillActor.class.getName());
    private ActorSelection spillMergeActor;
    private volatile int thread_count = 0;

    @Override
    public void preStart() throws Exception {
//        spillMergeActoy = getContext().actorOf(Props.create(SpillMergeActor.class), "SpillMergeActor");
        spillMergeActor = getContext().actorSelection("../SpillMergeActor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof List) {
            loger.info("溢写阶段接受到数据-------");
            for (KeyValue<String, Integer> item : (List<KeyValue<String, Integer>>) message) {
                mappedKeyValue.add(item);
            }
            ((List<KeyValue<String, Integer>>) message).clear();
            if (mappedKeyValue.size() > 40000) {
                loger.info("数据达到5000000准备进行溢写");
                List<KeyValue<String, Integer>> tmp = mappedKeyValue;
                mappedKeyValue = null;
                mappedKeyValue = new LinkedList<>();
//                    Collections.sort(tmp);
                KeyValue[] tmps = tmp.toArray(new KeyValue[0]);
                QuickSort<KeyValue<String, Integer>> quickSort = new QuickSort<KeyValue<String, Integer>>(tmps);
                quickSort.sort();
                ListIterator<KeyValue<String, Integer>> it = tmp.listIterator();
                for (KeyValue<String, Integer> e : tmps) {
                    it.next();
                    it.set(e);
                }
                spillMergeActor.tell(tmp, getSelf());
            }

        }
        if (message instanceof String) {
            if ("END".equals(message)) {
                if (mappedKeyValue.size() > 0) {
//                    Collections.sort(mappedKeyValue);
                    KeyValue[] tmps = mappedKeyValue.toArray(new KeyValue[0]);
                    QuickSort<KeyValue<String, Integer>> quickSort = new QuickSort<KeyValue<String, Integer>>(tmps);
                    quickSort.sort();
                    ListIterator<KeyValue<String, Integer>> it = mappedKeyValue.listIterator();
                    for (KeyValue<String, Integer> e : tmps) {
                        it.next();
                        it.set(e);
                    }
                    spillMergeActor.tell(mappedKeyValue,getSelf());
                    spillMergeActor.tell("SpillEnd",getSelf());
                }
            }
        }
    }

}
