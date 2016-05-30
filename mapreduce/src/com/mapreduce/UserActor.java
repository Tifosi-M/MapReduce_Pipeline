package com.mapreduce;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

/**
 * Created by szp on 16/5/27.
 */
public class UserActor extends UntypedActor {

    @Override
    public void preStart() throws Exception {
        final ActorRef readFileActor =
                getContext().actorOf(Props.create(ReadFileActor.class), "ReadFileActor");
        readFileActor.tell("start", getSelf());
    }

    @Override
    public void onReceive(Object o) throws Exception {

    }
}
