package com.tcp;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;

import java.net.InetSocketAddress;

/**
 * Created by szp on 2016/11/7.
 */
public class ServerActor extends UntypedActor {
    private ActorRef tcpActor;


    @Override
    public void preStart() throws Exception {
        if (tcpActor == null) {
            tcpActor = Tcp.get(getContext().system()).manager();
        }

        tcpActor.tell(TcpMessage.bind(getSelf(),
                new InetSocketAddress("localhost", 9090), 100), getSelf());
    }

    public static Props props(ActorRef tcpActor) {
        return Props.create(ServerActor.class, tcpActor);
    }

    public ServerActor(ActorRef tcpActor) {
        this.tcpActor = tcpActor;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Tcp.Bound) {
            System.out.println("In ServerActor - received message: bound");

        } else if (message instanceof Tcp.CommandFailed) {
            getContext().stop(getSelf());

        } else if (message instanceof Tcp.Connected) {
            final Tcp.Connected conn = (Tcp.Connected) message;
            System.out.println("In ServerActor - received message: connected");

            final ActorRef handler = getContext().actorOf(
                    Props.create(SimplisticHandlerActor.class));
            getSender().tell(TcpMessage.register(handler), getSelf());
        }else if (message instanceof String){
            if("stop".equals(message)){
                getContext().stop(getSelf());
            }
        }
    }
}
