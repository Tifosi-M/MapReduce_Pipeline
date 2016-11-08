package com.tcp;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Created by szp on 2016/11/7.
 */
public class SimplisticHandlerActor extends UntypedActor {

    ActorSelection MapphaseActor = getContext().actorSelection("akka://mapreduce/user/mapActor/");
    @Override
    public void onReceive(Object msg) throws Throwable {
        if (msg instanceof Tcp.Received) {
            RandomAccessFile raf = null;
            ByteBuffer bs = ((Tcp.Received) msg).data().toByteBuffer();

            File srcFile = new File("testData/spill_out/out2" + ".txt");
            FileOutputStream fileOutputStream = new FileOutputStream(srcFile,true);
//            try {
//                raf = new RandomAccessFile(srcFile, "rw");
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
            FileChannel fileChannel = fileOutputStream.getChannel();
            fileChannel.write(bs);
            fileChannel.close();
            fileOutputStream.close();
            bs.clear();

//            final String data = ((Tcp.Received) msg).data().utf8String();

//            System.out.println("In SimplisticHandlerActor - Received message: " + data);
//            getSender().tell(TcpMessage.write(ByteString.fromArray(("echo "+data).getBytes())), getSelf());
        } else if (msg instanceof Tcp.ConnectionClosed) {
            System.out.println("tcpClosed");
            MapphaseActor.tell("fileComplete",getSelf());
            getContext().stop(getSelf());
        }
    }
}
