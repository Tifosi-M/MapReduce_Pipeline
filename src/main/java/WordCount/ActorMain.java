package WordCount;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
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
        ActorSelection slave = system.actorSelection("akka.tcp://Map@127.0.0.1:51112/user/UserActor");
        userActor.tell("start",ActorRef.noSender());
        slave.tell("start",ActorRef.noSender());
    }
}
