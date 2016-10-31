package com.mapreduce;

import akka.actor.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by szp on 16/5/27.
 */
public class UserActor extends UntypedActor {
    private static ActorSystem system;
    ActorRef readFileActor;
    ActorRef mapActor;
    ActorRef spillActor;
    ActorRef groupActor;
    ActorRef spillMergeActor;
    ActorRef reduceActor;
    ActorRef spillActor2;
    ActorSelection cli;
    private static Logger logger = LogManager.getLogger(UserActor.class.getName());

    @Override
    public void preStart() throws Exception {
        logger.info("读取粒度40W,数据大小1G");
        logger.info("Actor启动");
        readFileActor = getContext().actorOf(Props.create(ReadFileActor.class), "ReadFileActor");
        mapActor = getContext().actorOf(Props.create(MapActor.class), "MapActor");
        spillActor = getContext().actorOf(Props.create(SpillActor.class), "SpillActor");
        spillMergeActor = getContext().actorOf(Props.create(SpillMergeActor.class), "SpillMergeActor");
        groupActor = getContext().actorOf(Props.create(GroupActor.class), "GroupActor");
        reduceActor = getContext().actorOf(Props.create(ReduceActor.class), "ReduceActor");
        cli = getContext().actorSelection("akka.tcp://test@127.0.0.1:5150/user/UserActor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            if ("start".equals(message))
                readFileActor.tell("start", getSelf());
//            cli.tell("test",ActorRef.noSender());
        }
    }

    public static void main(String[] args) {
        system = ActorSystem.create("actor-mapreduce-java");
        ActorRef userActor = system.actorOf(Props.create(UserActor.class), "UserActor");
        userActor.tell("start", ActorRef.noSender());
    }
}
