package com.mapreduce;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/5/27.
 */
public class ReadFileActor extends UntypedActor {
    ActorSelection mapActor = null;
    private static Logger logger = LogManager.getLogger(ReadFileActor.class.getName());

    @Override
    public void preStart() throws Exception {
        mapActor = getContext().actorSelection("../MapActor");

    }


    @Override
    public void onReceive(Object message) throws Exception {
        if (message.equals("start")) {
            int count = 0;
            List<KeyValue<Integer, String>> initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
            RandomAccessFile raf = new RandomAccessFile(new File("/root/input.txt"), "r");
            FileChannel fc = raf.getChannel();
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            StringBuffer sbf = new StringBuffer();
            while (mbb.remaining() > 0) {
                char data = (char) mbb.get();
                if (data != '\n') {
                    sbf.append(data);
                } else {
                    initialKeyValue.add(new KeyValue<Integer, String>(0, sbf.toString()));
                    sbf.setLength(0);
                    if (count == 800) {
                        mapActor.tell(initialKeyValue, getSelf());
                        initialKeyValue = null;
                        initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
                        count = 0;
                    }
                    count++;
                }
            }
            mapActor.tell(initialKeyValue, getSelf());
//            List<KeyValue<Integer, String>> initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
//            LineIterator it = FileUtils.lineIterator(new File("/root/input.txt"), "UTF-8");
//            logger.debug("开始读取文件==========================");
//            try {
//                while (it.hasNext()) {
//                    String line = it.nextLine();
//                    initialKeyValue.add(new KeyValue<Integer, String>(0, line));
//                    if (count == 800) {
//                        //发送消息给MapActoy
//                        mapActor.tell(initialKeyValue,getSelf());
//                        initialKeyValue = new ArrayList<KeyValue<Integer, String>>();
//                        count = 0;
//                    }
//                    count++;
//                }
//                mapActor.tell(initialKeyValue,getSelf());
//            } finally {
//                LineIterator.closeQuietly(it);
//            }
            mapActor.tell("END", getSelf());
            logger.info("文件读取结束");
            context().stop(getSelf());

        }

    }
}
