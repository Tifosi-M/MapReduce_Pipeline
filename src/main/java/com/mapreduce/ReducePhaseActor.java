package com.mapreduce;

import WordCount.MapWC;
import WordCount.ReduceWC;
import akka.actor.UntypedActor;

/**
 * Created by szp on 2016/11/3.
 */
public class ReducePhaseActor extends UntypedActor{
    MapReduce<Integer, String, String, Integer, String, Integer> wcMR =
            new MapReduce<Integer, String, String, Integer, String, Integer>(MapWC.class, ReduceWC.class, "MAP_REDUCE");

    private int sign = 0;//同步位
    @Override
    public void onReceive(Object message) throws Throwable {
        if(message instanceof String){
            if("startReduce".equals(message)){
                sign++;
                if(sign==2){
                    wcMR.startShuffle2();
                    wcMR.startReduce();
                    wcMR.writeToFile();
                }
            }
        }
    }
}
