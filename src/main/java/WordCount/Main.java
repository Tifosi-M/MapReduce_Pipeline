package WordCount;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.mapreduce.*;

public class Main {
	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create("mapreduce");
		ActorSelection slave = system.actorSelection("akka.tcp://map@127.0.0.1:5150/user/mapActor");
		ActorRef mapActor = system.actorOf(Props.create(MapPhaseActor.class),"mapActor");
		ActorRef reduceActor = system.actorOf(Props.create(ReducePhaseActor.class),"reduceActor");
		mapActor.tell("start", ActorRef.noSender());
		slave.tell("start",ActorRef.noSender());
	}


}
