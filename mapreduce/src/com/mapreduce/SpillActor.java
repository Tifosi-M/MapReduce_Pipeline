package com.mapreduce;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by szp on 16/5/27.
 */
public class SpillActor extends UntypedActor {
    private List<KeyValue<String, Integer>> mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
    private int count = 0;
    private static Logger loger = LogManager.getLogger(SpillActor.class.getName());
    private ActorRef spillMergeActoy;

    @Override
    public void preStart() throws Exception {
        spillMergeActoy = getContext().actorOf(Props.create(SpillMergeActor.class), "SpillMergeActor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
//        loger.debug("写入次数第"+count+"次");
        if (message instanceof List) {
            for (KeyValue<String, Integer> item : (List<KeyValue<String, Integer>>) message) {
                mappedKeyValue.add(item);
            }
            if (mappedKeyValue.size() > 5000000) {
                loger.debug("排序中");
                Collections.sort(mappedKeyValue);
                loger.debug("正在写入文件" + count);
                File srcFile = new File("/Users/szp/Documents/github/MapReduce_Pipeline/mapreduce/spill_out/" + count + ".txt");
                try {
                    for (int i = 0; i < mappedKeyValue.size(); i++) {
                        KeyValue<String, Integer> keyValue = mappedKeyValue.remove(0);
                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
                loger.debug("单次写入结束" + count);
                count++;
            }
            if (count == 2) {
                spillMergeActoy.tell("StartMerge", getSelf());
            }
            if (count > 2) {
                spillMergeActoy.tell("Merge", getSelf());
            }

        }
        if (message instanceof String) {
            if ("END".equals((String) message)) {
                loger.debug("排序中");
                Collections.sort(mappedKeyValue);
                loger.debug("正在写入文件" + count);
                File srcFile = new File("/Users/szp/Documents/github/MapReduce_Pipeline/mapreduce/spill_out/" + count + ".txt");

                try {
                    for (int i = 0; i < mappedKeyValue.size(); i++) {
                        KeyValue<String, Integer> keyValue = mappedKeyValue.remove(0);
                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
                loger.debug("单次写入结束" + count);
                count++;
                if (count == 2) {
                    spillMergeActoy.tell("StartMerge", getSelf());
                }
                if (count > 2) {
                    spillMergeActoy.tell("Merge", getSelf());
                }
                spillMergeActoy.tell("END",getSelf());
            }
            loger.debug("全部处理完成");
        }

    }
//        System.gc();

}
