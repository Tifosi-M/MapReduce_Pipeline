package com.mapreduce;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by szp on 16/6/1.
 */
public class GroupActor extends UntypedActor {
    private List<KeyValue<String, Integer>> list;
    private List<GroupedKeyValue<String, Integer>> gKVList = new ArrayList<GroupedKeyValue<String, Integer>>();
    private ActorSelection reduceActor;
    private static Logger logger = LogManager.getLogger(GroupActor.class.getName());
    private Queue<List<KeyValue<String, Integer>>> queue = new LinkedList<List<KeyValue<String, Integer>>>();
    private int sign = 0;//栅栏同步标记位
    private int test = 0;
    volatile int threadcount = 0;

    @Override
    public void preStart() throws Exception {
//        reduceActory = getContext().actorOf(Props.create(ReduceActor.class),"ReduceActor");
        reduceActor = getContext().actorSelection("../ReduceActor");
    }

    public List mergeList(List<KeyValue<String, Integer>> list_1, List<KeyValue<String, Integer>> list_2) {
        List<KeyValue<String, Integer>> list_out = new ArrayList<>();
        while (list_1.size() != 0 || list_2.size() != 0) {
            if (list_1.size() != 0 && list_2.size() != 0) {
                KeyValue<String, Integer> keyValue1 = list_1.get(0);
                KeyValue<String, Integer> keyValue2 = list_2.get(0);
                if (keyValue1.compareTo(keyValue2) < 0) {
                    list_out.add(keyValue1);
                    list_1.remove(0);
                } else {
                    list_out.add(keyValue2);
                    list_2.remove(0);
                }
            } else {
                if (list_1.size() == 0) {
                    list_out.add(list_2.remove(0));
                } else {
                    list_out.add(list_1.remove(0));
                }
            }
        }
        return list_out;
    }

    private void register() {
        synchronized (this) {
            threadcount++;
        }
    }

    private void unregister() {
        synchronized (this) {
            threadcount--;
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof List) {
            queue.add((List<KeyValue<String, Integer>>) message);
        }
        if (message instanceof String) {
            if ("MergeEnd".equals(message)) {
                sign++;
                if (sign == 1) {
                    int list_count = queue.size();
                    do {
                        int tmp = list_count;
                        for (int i = 0; i < tmp / 2; i++) {
                            new Thread(() -> {
                                register();
                                queue.add(mergeList(queue.poll(), queue.poll()));
                                unregister();
                            }).start();
                            list_count--;
                            System.out.println(queue.size());
                        }//hello my order is . could you help me inquiry the track number of chinese carrier.
                        while (threadcount != 0)
                            Thread.sleep(5000);
                    } while (list_count > 1);

                    list = queue.poll();
                    String tmpKey;
                    GroupedKeyValue<String, Integer> gkv;


                    String conKey = list.get(0).getKey();
                    gkv = new GroupedKeyValue<String, Integer>(conKey, new GroupedValues<Integer>(list.get(0).getValue()));

                    for (int i = 1; i < list.size(); i++) {
                        tmpKey = list.get(i).getKey();

                        if (tmpKey.equals(gkv.getKey())) {
                            gkv.addValue(list.get(i).getValue());
                        } else {
                            gKVList.add(gkv);
                            conKey = tmpKey;
                            gkv = new GroupedKeyValue<String, Integer>(conKey, new GroupedValues<Integer>(list.get(i).getValue()));
                        }
                    }
                    gKVList.add(gkv);
                    logger.info("Grouping 阶段结束");
                    reduceActor.tell(gKVList, getSelf());

                }
            }
//            if ("StartGrouping".equals(message)) {
//                logger.info("Grouping阶段开始");
//                File file = new File("testData/spill_out");
//                String[] names = file.list(new SuffixFileFilter(".txt"));
//
//                File srcFile = new File("testData/spill_out/" + names[0]);
//                LineIterator it = null;
//                try {
//                    it = FileUtils.lineIterator(srcFile, "UTF-8");
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                try {
//                    while (it.hasNext()) {
//                        String line = it.nextLine();
//                        String[] str = line.split(" ");
//                        list.add(new KeyValue<String, Integer>(str[0], (Integer.valueOf(str[1]))));
//                    }
//                } catch (ArrayIndexOutOfBoundsException e) {
//                    e.printStackTrace();
//
//                } finally {
//                    LineIterator.closeQuietly(it);
//                }
//                String tmpKey;
//                GroupedKeyValue<String, Integer> gkv;
//
//
//                String conKey = list.get(0).getKey();
//                gkv = new GroupedKeyValue<String, Integer>(conKey, new GroupedValues<Integer>(list.get(0).getValue()));
//
//                for (int i = 1; i < list.size(); i++) {
//                    tmpKey = list.get(i).getKey();
//
//                    if (tmpKey.equals(gkv.getKey())) {
//                        gkv.addValue(list.get(i).getValue());
//                    } else {
//                        gKVList.add(gkv);
//                        conKey = tmpKey;
//                        gkv = new GroupedKeyValue<String, Integer>(conKey, new GroupedValues<Integer>(list.get(i).getValue()));
//                    }
//                }
//                gKVList.add(gkv);
//                logger.info("Grouping 阶段结束");
//                reduceActor.tell(gKVList, getSelf());
//                context().stop(getSelf());
//            }
        }
    }
}
