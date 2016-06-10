package com.mapreduce;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
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
    private ActorSelection spillMergeActor;

    @Override
    public void preStart() throws Exception {
//        spillMergeActoy = getContext().actorOf(Props.create(SpillMergeActor.class), "SpillMergeActor");
        spillMergeActor = getContext().actorSelection("../SpillMergeActor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof List) {
            for (KeyValue<String, Integer> item : (List<KeyValue<String, Integer>>) message) {
                mappedKeyValue.add(item);
            }
            if (mappedKeyValue.size() > 2500000) {
                Collections.sort(mappedKeyValue);
                loger.debug("正在写入文件" + count);
                File srcFile = new File("/root/spill_out/" + count + ".txt");
                try {
                    for (int i = 0; i < mappedKeyValue.size(); i++) {
                        KeyValue<String, Integer> keyValue = mappedKeyValue.remove(0);
                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
                loger.debug("文件" + count+"写入结束");
                count++;
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
                Collections.sort(mappedKeyValue);
                loger.debug("正在写入文件" + count);
                File srcFile = new File("/root/spill_out/" + count + ".txt");

                try {
                    for (int i = 0; i < mappedKeyValue.size(); i++) {
                        KeyValue<String, Integer> keyValue = mappedKeyValue.remove(0);
                        FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                mappedKeyValue = new LinkedList<KeyValue<String, Integer>>();
                loger.debug("文件" + count+"写入结束");
                count++;
//                if (count == 2) {
//                    spillMergeActor.tell("StartMerge", getSelf());
//                }
//                if (count > 2) {
//                    spillMergeActor.tell("Merge", getSelf());
//                }
                spillMergeActor.tell("StartMerge",getSelf());
                loger.info("溢写完成");
                context().stop(getSelf());
            }

        }

    }

}
