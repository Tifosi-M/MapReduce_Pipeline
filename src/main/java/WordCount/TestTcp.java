package WordCount;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.tcp.ServerActor;

/**
 * Created by szp on 2016/11/7.
 */
public class TestTcp {
    public static void main(String[] args){
        ActorSystem serverActorSystem = ActorSystem.create("ServerActorSystem");
        ActorRef serverActor = serverActorSystem.actorOf(ServerActor.props(null), "serverActor");
    }
}
