package com.mapreduce;

import WordCount.ReduceWC;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/6/1.
 */
public class ReduceActor extends UntypedActor {
    private Class<? extends Reducer<String, Integer, String, Integer>> reduceClass;
    private static Logger logger = LogManager.getLogger(ReduceActor.class.getName());
    private OutputData<String, Integer> outputData =new OutputData<String, Integer>();
    private ActorSelection selection = getContext().actorSelection("../UserActor");
    Reducer<String, Integer, String, Integer> initializeReducer() {
        try {
            return ReduceWC.class.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof List){
            logger.info("开始Reducer");
            List<GroupedKeyValue<String, Integer>> gKVList = (ArrayList<GroupedKeyValue<String, Integer>>)message;
            Reducer reducer = initializeReducer();
            for(int i =0;i<gKVList.size();i++){
                reducer.setKeyValue(gKVList.get(i).getKey(), gKVList.get(i).getValues());
                reducer.reduce();
                outputData.setKeyValue((String)reducer.getKey(), (Integer)reducer.getValue());
            }
            logger.info("Reduce阶段结束========================");
            outputData.writeToFile();
        }
    }
}
