package com.mapreduce;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/6/1.
 */
public class GroupActor extends UntypedActor {
    private List<KeyValue<String, Integer>> list = new ArrayList<KeyValue<String, Integer>>();
    private List<GroupedKeyValue<String, Integer>> gKVList = new ArrayList<GroupedKeyValue<String, Integer>>();
    private ActorSelection reduceActor;
    private static Logger logger = LogManager.getLogger(GroupActor.class.getName());

    @Override
    public void preStart() throws Exception {
//        reduceActory = getContext().actorOf(Props.create(ReduceActor.class),"ReduceActor");
        reduceActor = getContext().actorSelection("../ReduceActor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof String) {
            if ("StartGrouping".equals((String)message)) {
                logger.info("Grouping阶段开始");
                File srcFile = new File("/root/spill_out/out.txt");
                LineIterator it = null;
                try {
                    it = FileUtils.lineIterator(srcFile, "UTF-8");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    while (it.hasNext()) {
                        String line = it.nextLine();
                        String[] str = line.split(" ");
                        list.add(new KeyValue<String, Integer>((String) str[0], (Integer.valueOf(str[1]))));
                    }
                }catch (ArrayIndexOutOfBoundsException e){
                    e.printStackTrace();

                }finally {
                    LineIterator.closeQuietly(it);
                }
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
                reduceActor.tell(gKVList,getSelf());
                context().stop(getSelf());
            }
        }
    }
}
