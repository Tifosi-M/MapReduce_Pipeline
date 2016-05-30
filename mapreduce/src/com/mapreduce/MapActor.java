package com.mapreduce;

import WordCount.MapWC;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szp on 16/5/27.
 */

public class MapActor extends UntypedActor {
    private Class<? extends Mapper<Integer, String, String, Integer>> mapClass;
    private InputData<Integer, String, String, Integer> inputData;
    //    public MapActor(Class<? extends Mapper<Integer, String, String, Integer>> map_class){
//        this.mapClass = map_class;
//    }
    private ActorRef spillActoy;
    private static Logger logger = LogManager.getLogger(ReadFileActor.class.getName());

    @Override
    public void preStart() throws Exception {
        spillActoy = getContext().actorOf(Props.create(SpillActor.class), "SpillActor");
    }

    Mapper<Integer, String, String, Integer> initializeMapper() {
        try {
            return MapWC.class.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof List) {
            inputData = new InputData<Integer, String, String, Integer>();
            Mapper mapper = initializeMapper();
            inputData.initialKeyValue = (ArrayList<KeyValue<Integer, String>>) message;
            for (int i = 0; i < inputData.getMapSize(); i++) {
                mapper.setKeyValue(inputData.getMapKey(i), inputData.getMapValue(i));
                mapper.map();
            }
            List<String> resultMapKeys = mapper.getKeys();
            List<Integer> resultMapValues = mapper.getValues();
            for (int j = 0; j < resultMapKeys.size(); j++) {
                inputData.setMap(resultMapKeys.get(j), resultMapValues.get(j));
            }
            logger.debug("mappedKeyValue数据量:"+inputData.mappedKeyValue.size());

            spillActoy.tell(inputData.mappedKeyValue, getSelf());
            logger.debug("Map结束");
        }
        if(message instanceof String){
            spillActoy.tell(message,getSelf());
        }
    }
}
