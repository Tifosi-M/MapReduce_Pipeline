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

/**
 * Created by szp on 16/6/1.
 */
public class SpillMergeActor extends UntypedActor {
    private static Logger loger = LogManager.getLogger(SpillMergeActor.class.getName());
    private ActorSelection groupActor;
    private int uniqueCount = 100;

    @Override
    public void preStart() throws Exception {
//        groupActor = getContext().actorOf(Props.create(GroupActor.class),"GroupActor");
        groupActor = getContext().actorSelection("../GroupActor");
    }

    public void mergeFile(String filename1, String filename2) {
        LineIterator it1 = null;
        LineIterator it2 = null;
        LinkedList<KeyValue<String, Integer>> list_1 = new LinkedList<KeyValue<String, Integer>>();
        LinkedList<KeyValue<String, Integer>> list_2 = new LinkedList<KeyValue<String, Integer>>();
        LinkedList<KeyValue<String, Integer>> list_out = new LinkedList<KeyValue<String, Integer>>();
        File file1 = new File(filename1);
        File file2 = new File(filename2);
        try {
            it1 = FileUtils.lineIterator(file1, "UTF-8");
            it2 = FileUtils.lineIterator(file2, "UTF-8");

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            while (it1.hasNext()) {
                String line = it1.nextLine();
                String[] str = line.split(" ");
                String key = (String) str[0];
                Integer value = Integer.parseInt(str[1]);
                list_1.add(new KeyValue<String, Integer>(key, value));
            }
        } finally {
            LineIterator.closeQuietly(it1);
        }
        try {
            while (it2.hasNext()) {
                String line = it2.nextLine();
                String[] str = line.split(" ");
                String key = (String) str[0];
                Integer value = Integer.parseInt(str[1]);
                list_2.add(new KeyValue<String, Integer>(key, value));
            }
        } finally {
            LineIterator.closeQuietly(it2);
        }
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

        file1.delete();
        file2.delete();

        File srcFile = new File("testData/spill_out/out" + uniqueCount++ + ".txt");
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(srcFile, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        FileChannel fileChannel = raf.getChannel();
        ByteBuffer rBuffer = ByteBuffer.allocateDirect(512 * 1024 * 1024);
        try {
            int size = list_out.size();
            for (int i = 0; i < size; i++) {
                KeyValue<String, Integer> keyValue = list_out.remove(0);
                rBuffer.put((keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n").getBytes());
                if (rBuffer.limit() == rBuffer.capacity() - 2) {
                    rBuffer.flip();
                    fileChannel.write(rBuffer);
                    rBuffer.clear();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        rBuffer.flip();
        try {
            fileChannel.write(rBuffer);
            fileChannel.close();
            raf.close();
            rBuffer.clear();
            if (rBuffer == null) return;
            Cleaner cleaner = ((DirectBuffer) rBuffer).cleaner();
            if (cleaner != null) cleaner.clean();
        } catch (IOException e) {
            e.printStackTrace();
        }


//            File file = new File("testData/spill_out/out" + uniqueCount++ + ".txt");
//            for (int i = 0; i < list_out.size(); i++) {
//                KeyValue<String, Integer> keyValue = list_out.remove(0);
//                FileUtils.writeStringToFile(file, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public File[] getFiles(String path) {
        File file = new File(path);
        File[] fileList = file.listFiles();
        return fileList;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            loger.info("开始进行溢写合并");
            if ("StartMerge".equals((String) message)) {
                int filecount = 0;
                do {
                    File file = new File("testData/spill_out");
                    List<String> filenames = new ArrayList<String>();
                    File[] files = file.listFiles((FilenameFilter) new SuffixFileFilter(".txt"));
                    for (File txtfile : files) {
                        filenames.add(txtfile.toString());
                    }
                    filecount = filenames.size();
                    for (int i = 0; i < filecount / 2; i++) {
                        mergeFile(filenames.remove(0), filenames.remove(0));
                        filecount--;
                    }
                    filenames.clear();

                } while (filecount >= 2);

                groupActor.tell("StartGrouping", getSelf());
                loger.info("溢写合并完成");
                context().stop(getSelf());
            }
//            if("END".equals((String)message)){
//                groupActor.tell("StartGrouping",getSelf());
//                loger.info("溢写合并完成");
//                context().stop(getSelf());
//            }
        }
    }
}
