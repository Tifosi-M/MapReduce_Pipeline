package com.mapreduce;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
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
import java.util.*;
import java.util.concurrent.*;


public class MapReduce<InputMapKey extends Comparable<InputMapKey>, InputMapValue, IntermediateKey extends Comparable<IntermediateKey>, IntermediateValue, OutputReduceKey, OutputReduceValue> {

    private Class<? extends Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>> mapClass;
    private Class<? extends Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>> reduceClass;

    private InputData<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue> inputData;
    private OutputData<OutputReduceKey, OutputReduceValue> outputData;
    private static Logger logger = LogManager.getLogger(MapReduce.class.getName());
    private int spillfileCount = 0;
    private List<SpillThread>list_thread = new ArrayList<SpillThread>();

	/*
     * phase_mp变量确定执行Mapreduce的哪个阶段
	 * phase_mp为"MAP_ONLY"只进行map处理
	 * "MAP_SHUFFLE"为进行Map和Shuffle,
	 * "MAP_REDUCE"执行Map、Shuffle、Reduce三个阶段
	 * 除此之外的值视为"MAP_REDUCE"
	 */

    private String phaseMR;

    //
    private boolean resultOutput;

    //The Number of concurrent threads
    private int parallelThreadNum;

    public MapReduce(
            Class<? extends Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>> map_class,
            Class<? extends Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>> reduce_class,
            String phase_mp
    ) {
        this.mapClass = map_class;
        this.reduceClass = reduce_class;
        this.inputData = new InputData<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>();
        this.outputData = new OutputData<OutputReduceKey, OutputReduceValue>();
        this.phaseMR = phase_mp;
        this.resultOutput = true;
        this.parallelThreadNum = 1;
    }

    public void setPhaseMR(String phaseMR) {
        this.phaseMR = phaseMR;
    }

    public void setResultOutput(boolean resultOutput) {
        this.resultOutput = resultOutput;
    }

    public void setParallelThreadNum(int num) {
        this.parallelThreadNum = num;
    }
	
	
	/*
	 * addKeyValue
	 * pass data formated as key-value pairs to inputData
	 */

    public void addKeyValue(InputMapKey key, InputMapValue value) {
        this.inputData.putKeyValue(key, value);
    }

    /**
     * 运行Map
     * 对每个Key-Value执行以下过程
     * 1.Mapper实例
     * 2.1通过FutureTask生成实例
     * 3.把结果保存在inputdata中，并行运行MapWork
     * 4.1.-3.重复运行
     */
    public void startMap() {
        logger.info("Map阶段启动");
        List<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>> mappers = new ArrayList<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>(this.parallelThreadNum);
        List<FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>> maptasks = new ArrayList<FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>>(this.parallelThreadNum);
        ExecutorService executor = Executors.newFixedThreadPool(this.parallelThreadNum);
        //将剩余所有initKeyValue存入InitialKeyValue_list中
//        inputData.putInitialKeyValueToInitialKeyValue_queue();
        for (int i = 0; i < this.parallelThreadNum; i++) {
            mappers.add(initializeMapper());
            maptasks.add(new FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>(new MapCallable<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>()));
        }


//        for (int k=0;k<this.inputData.initialKeyValue_queue.size();k++) {
//            inputData.getKeyValueFromQueue();

        int x = this.inputData.getMapSize() / this.parallelThreadNum;//每个线程处理的inputdata键值对个数
        for (int i = 0; i < this.inputData.getMapSize() / this.parallelThreadNum; i++) {
            for (int j = 0; j < this.parallelThreadNum; j++) {
                mappers.set(j, initializeMapper());
                mappers.get(j).setKeyValue(this.inputData.getMapKey(i + j * x), this.inputData.getMapValue(i + j * x));
                maptasks.set(j, new FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>(new MapCallable<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>(mappers.get(j))));
            }

            try {
                MapWork(mappers, maptasks, executor);
            } catch (OutOfMemoryError e) {
                System.out.println(i);
                System.exit(1);
            }
        }

        if (this.inputData.getMapSize() % this.parallelThreadNum != 0) {
            int finishedsize = (this.inputData.getMapSize() / this.parallelThreadNum) * this.parallelThreadNum;
            for (int i = 0; i < this.inputData.getMapSize() % this.parallelThreadNum; i++) {
                mappers.set(i, initializeMapper());
                mappers.get(i).setKeyValue(this.inputData.getMapKey(finishedsize + i), this.inputData.getMapValue(finishedsize + i));
                maptasks.set(i, new FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>(new MapCallable<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>(mappers.get(i))));
            }

            MapWork(mappers, maptasks, executor);
        }


//            inputData.initialKeyValue.clear();
//        }
        //inputData.serializeMapOutData();
        mappers = null;
        maptasks = null;
        //完成Map阶段所有计算释放map阶段中initialKeyValue键值对的存储空间
        this.inputData.initialRelease();
        executor.shutdown();

        logger.debug("Map阶段结束");

    }

    private void MapWork(
            List<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>> mappers,
            List<FutureTask<Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue>>> maptasks,
            ExecutorService executor
    ) {

        for (int i = 0; i < this.parallelThreadNum; i++) {
            executor.submit(maptasks.get(i));
        }

        //Map函数的输出
        try {
            for (int i = 0; i < this.parallelThreadNum; i++) {
                List<IntermediateKey> resultMapKeys = maptasks.get(i).get().getKeys();
                List<IntermediateValue> resultMapValues = maptasks.get(i).get().getValues();
                for (int j = 0; j < resultMapKeys.size(); j++) {
                    if (this.inputData.getMappedKeyValueSize() > 2500000) {
                        SpillThread thread = new SpillThread();
                        thread.setSpill_list(inputData.mappedKeyValue);
                        thread.setCount(++spillfileCount);
                        inputData.mappedKeyValue = inputData.instanceMapedKeyValue();
                        list_thread.add(thread);
                        thread.start();
                    }
                    this.inputData.setMap(resultMapKeys.get(j), resultMapValues.get(j));
                }
            }
        } catch (InterruptedException e) {
            e.getCause().printStackTrace();
        } catch (ExecutionException e) {
            e.getCause().printStackTrace();
        }
    }


    /**
     * 初始化Mapper子类
     *
     * @return Mapper子类的实例
     */
    Mapper<InputMapKey, InputMapValue, IntermediateKey, IntermediateValue> initializeMapper() {
        try {
            return mapClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ReducePhases
     * 1.排序
     * 2.合并
     */
    public void startShuffle() {
        spillReamin();
        for(SpillThread thread:list_thread){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("溢写合并开始");
        this.spillMerge();
        logger.debug("溢写合并结束");
        logger.debug("Grouping 开始");
        this.grouping();
        logger.debug("Grouping 结束");
    }


    /**
     * 运行Reduce
     * 对每个Key-Value执行以下过程
     * 1.Reducer实例
     * 2.1.通过FutureTask生成实例
     * 3.ReduceWor运行结果保存在outputData中
     * 4.1.-3.重复运行
     */
    public void startReduce() {
        logger.info("Reduce阶段开始");
        List<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>> reducers = new ArrayList<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>(this.parallelThreadNum);
        List<FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>> reducetasks = new ArrayList<FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>>(this.parallelThreadNum);
        ExecutorService executor = Executors.newFixedThreadPool(this.parallelThreadNum);

        for (int i = 0; i < this.parallelThreadNum; i++) {
            reducers.add(initializeReducer());
            reducetasks.add(new FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>(new ReduceCallable<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>()));
        }

        int x = this.inputData.getReduceSize() / this.parallelThreadNum;
        for (int i = 0; i < this.inputData.getReduceSize() / this.parallelThreadNum; i++) {
            for (int j = 0; j < this.parallelThreadNum; j++) {
                reducers.set(j, initializeReducer());
                reducers.get(j).setKeyValue(this.inputData.getReduceKey(i + j * x), this.inputData.getReduceValues(i + j * x));
                reducetasks.set(j, new FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>(new ReduceCallable<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>(reducers.get(j))));
            }

            ReduceWork(reducers, reducetasks, executor);
        }


        if (this.inputData.getReduceSize() % this.parallelThreadNum != 0) {
            int finishedsize = (this.inputData.getReduceSize() / this.parallelThreadNum) * this.parallelThreadNum;
            for (int i = 0; i < this.inputData.getReduceSize() % this.parallelThreadNum; i++) {
                reducers.set(i, initializeReducer());
                reducers.get(i).setKeyValue(this.inputData.getReduceKey(finishedsize + i), this.inputData.getReduceValues(finishedsize + i));
                reducetasks.set(i, new FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>(new ReduceCallable<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>(reducers.get(i))));
            }

            ReduceWork(reducers, reducetasks, executor);
        }

        reducers = null;
        reducetasks = null;
        executor.shutdown();
        logger.info("Reduce阶段结束====================");
    }

    /**
     * 并行执行ReduceWork
     *
     * @param reducers
     * @param reducetasks
     * @param executor
     */
    private void ReduceWork(
            List<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>> reducers,
            List<FutureTask<Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue>>> reducetasks,
            ExecutorService executor
    ) {


        for (int i = 0; i < this.parallelThreadNum; i++) {
            executor.submit(reducetasks.get(i));
        }

        try {
            for (int j = 0; j < this.parallelThreadNum; j++) {
                OutputReduceKey resultMapKey = reducetasks.get(j).get().getKey();
                OutputReduceValue resultMapValue = reducetasks.get(j).get().getValue();
                this.outputData.setKeyValue(resultMapKey, resultMapValue);
            }
        } catch (InterruptedException e) {
            e.getCause().printStackTrace();
        } catch (ExecutionException e) {
            e.getCause().printStackTrace();
        }
    }

    /**
     * 初始化Reducer子类
     *
     * @return Reducer子类的实例
     */
    Reducer<IntermediateKey, IntermediateValue, OutputReduceKey, OutputReduceValue> initializeReducer() {
        try {
            return reduceClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 执行MapReduce
     */
    public void run() {
//        startMap();
//
//        if (this.phaseMR.equals("MAP_ONLY")) {
//            if (this.resultOutput)
//                //inputData.showMap();
//            return;
//        }
//
//        startShuffle();
//        if (this.phaseMR.equals("MAP_SHUFFLE")) {
//            if (this.resultOutput)
//                inputData.showSuffle();
//            return;
//        }
//        startReduce();
//        if (this.resultOutput)
//            outputData.reduceShow();
        logger.debug("==================启动Map阶段=======================");
        startMap();
        logger.debug("==================Map阶段结束=======================");
        logger.debug("================启动Shuffle阶段=====================");
        startShuffle();
        logger.debug("==================Shuffle阶段结束===================");
        logger.debug("==================启动Reduce阶段====================");
        startReduce();
        logger.debug("==================Reduce阶段结束====================");


    }

    public void writeToFile() {
        outputData.writeToFile();
    }


    /**
     * 获取MapReduce处理完成后的key集合
     */
    public List<OutputReduceKey> getKeys() {
        return this.outputData.getOutputKeys();
    }

    /**
     * 获取MapReduce处理完成后的value集合
     */
    public List<OutputReduceValue> getValues() {
        return this.outputData.getOutputValues();
    }

    class SpillThread extends Thread {
        private List<KeyValue<IntermediateKey, IntermediateValue>> spill_list;
        private int count;
        public void setSpill_list(List<KeyValue<IntermediateKey, IntermediateValue>> list) {
            spill_list = list;
        }
        public void setCount(int count){this.count = count;}
        @Override
        public void run() {
//            Collections.sort(spill_list);
            logger.debug("缓存溢写文件"+count+"开始");
            File srcFile = new File("testData/spill_out/"+count + ".txt");
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(srcFile, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel fileChannel = raf.getChannel();
            ByteBuffer rBuffer = ByteBuffer.allocateDirect(32 * 1024 * 1024);
            try {
                int size = spill_list.size();
                for (int i = 0; i < size; i++) {
                    KeyValue<IntermediateKey,IntermediateValue> keyValue = spill_list.remove(0);
//                    FileUtils.writeStringToFile(srcFile, keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
                    rBuffer.put((keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n").getBytes());
                }
                rBuffer.flip();
                fileChannel.write(rBuffer);
                fileChannel.close();
                raf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            spill_list = null;
            logger.info("文件"+count+"溢写结束");
        }
    }
    public void spillReamin(){
        SpillThread thread = new SpillThread();
        thread.setSpill_list(inputData.mappedKeyValue);
        thread.setCount(++spillfileCount);
        list_thread.add(thread);
        thread.start();
    }

    public void spillMerge() {
        File[] files = getFiles("testData/spill_out");
        try {
            Files.createFile(Paths.get("testData/spill_out/out.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (File file :files){
            if(!file.getName().split("\\.")[0].equals("")&&!file.getName().split("\\.")[0].equals("out")){
                mergeFile("testData/spill_out/out.txt",file.toString());
            }
        }
        logger.debug("溢写合并结束");
    }
    public void grouping() {
        File srcFile = new File("testData/spill_out/out.txt");
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
                IntermediateKey key = (IntermediateKey) str[0];
                IntermediateValue value = (IntermediateValue)(Integer.valueOf(str[1]));
                this.inputData.list.add(new KeyValue<IntermediateKey, IntermediateValue>(key, value));
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();

        }finally {
            LineIterator.closeQuietly(it);
        }
        inputData.grouping();
    }
    public void mergeFile(String filename1,String filename2){
        LineIterator it1 = null;
        LineIterator it2 = null;
        LinkedList<KeyValue<String,Integer>> list_1 = new LinkedList<KeyValue<String,Integer>>();
        LinkedList<KeyValue<String,Integer>> list_2 = new LinkedList<KeyValue<String,Integer>>();
        LinkedList<KeyValue<String,Integer>> list_out = new LinkedList<KeyValue<String,Integer>>();
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
            }else{
                if(list_1.size()==0){
                    list_out.add(list_2.remove(0));
                }else{
                    list_out.add(list_1.remove(0));
                }
            }
        }
        if(file1.getName().equals("0.txt"))
            file1.delete();
        file2.delete();

        try {
            File file = new File("testData/spill_out/out.txt");
            for (int i = 0; i < list_out.size(); i++) {
                KeyValue<String, Integer> keyValue = list_out.remove(0);
                FileUtils.writeStringToFile(file,keyValue.getKey().toString() + " " + keyValue.getValue().toString() + "\n", "utf-8", true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public File[] getFiles(String path){
        File file = new File(path);
        File[] fileList = file.listFiles();
        return fileList;
    }


}
