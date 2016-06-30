package WordCount;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.mapreduce.UserActor;

/**
 * Created by szp on 16/5/27.
 */
public class ActorMain {
    public static void main(String[] args){
        ActorSystem system = ActorSystem.create("actor-mapreduce-java");
        ActorRef userActor = system.actorOf(Props.create(UserActor.class),"UserActor");
        userActor.tell("start",ActorRef.noSender());
    }
}
