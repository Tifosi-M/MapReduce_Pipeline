package com.mapreduce;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by szp on 16/6/1.
 */
public class SpillMergeActor extends UntypedActor {
    private static Logger loger = LogManager.getLogger(SpillMergeActor.class.getName());
    private ActorSelection groupActor;
    private int count = 0;
    private Queue<List<KeyValue<String, Integer>>> queue = new LinkedList<List<KeyValue<String, Integer>>>();

    @Override
    public void preStart() throws Exception {
//        groupActor = getContext().actorOf(Props.create(GroupActor.class),"GroupActor");
        groupActor = getContext().actorSelection("../GroupActor");
    }

    public List mergeList(List<KeyValue<String, Integer>> list_1, List<KeyValue<String, Integer>> list_2) {
        List<KeyValue<String, Integer>> list_out = new ArrayList<>();
        int i = 0, j = 0;
        int list1_size = list_1.size();
        int list2_size = list_2.size();
        while (list1_size != 0 || list2_size != 0) {
            if (list1_size != 0 && list2_size != 0) {
                KeyValue<String, Integer> keyValue1 = list_1.get(i);
                KeyValue<String, Integer> keyValue2 = list_2.get(j);
                if (keyValue1.compareTo(keyValue2) < 0) {
                    list_out.add(keyValue1);
                    i++;
                    list1_size--;
                } else {
                    list_out.add(keyValue2);
                    j++;
                    list2_size--;
                }
            } else {
                if (list1_size == 0) {
                    list_out.add(list_2.get(j));
                    j++;
                    list2_size--;
                } else if (list2_size == 0) {
                    list_out.add(list_1.get(i));
                    i++;
                    list1_size--;
                }
            }
        }
        return list_out;
    }


    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            loger.info("开始进行溢写合并");
            if ("SpillEnd".equals(message)) {
                for(int i =0 ;i<queue.size();i++)
                    groupActor.tell(queue.poll(),getSelf());
                groupActor.tell("MergeEnd", getSelf());
                context().stop(getSelf());
            }
        }
        if (message instanceof List) {
            count++;
            queue.add((List<KeyValue<String, Integer>>) message);
            if (count == 10) {
                int list_count = count;
                count = 0;
                //对所有List中的数据进行merge,merge结束后发送给GroupActor
                do {
                    int tmp = list_count;
                    for (int i=0; i < tmp / 2; i++) {
                        queue.add(mergeList(queue.poll(), queue.poll()));

                        list_count--;
                    }
                } while (list_count>1);
                groupActor.tell(queue.poll(),getSelf());
            }
        }
    }
}
