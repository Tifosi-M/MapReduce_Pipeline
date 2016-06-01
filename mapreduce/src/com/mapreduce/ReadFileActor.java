package com.mapreduce;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/5/27.
 */
public class ReadFileActor extends UntypedActor{
    ActorRef mapActor = null;
    private static Logger logger = LogManager.getLogger(ReadFileActor.class.getName());
    @Override
    public void preStart() throws Exception {
         mapActor = getContext().actorOf(Props.create(MapActor.class), "MapActor");
    }


    @Override
    public void onReceive(Object message) throws Exception {
        if(message.equals("start")){
            int count=0;
            List<KeyValue<Integer, String>> initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
            LineIterator it = FileUtils.lineIterator(new File("test.txt"), "UTF-8");
            try {
                while (it.hasNext()) {
                    String line = it.nextLine();
                    initialKeyValue.add(new KeyValue<Integer, String>(0, line));
                    if (count == 80000) {
//                        logger.debug("发送读取数据，数据大小:"+initialKeyValue.size());
                        //发送消息给MapActoy
//                        logger.debug("readFile:"+initialKeyValue.size());
                        mapActor.tell(initialKeyValue,getSelf());
                        initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
                        count = 0;
                    }
                    count++;
                }
                mapActor.tell(initialKeyValue,getSelf());
            } finally {
                LineIterator.closeQuietly(it);
            }
            mapActor.tell("END",getSelf());

        }

    }
}
