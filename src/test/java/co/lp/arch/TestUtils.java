/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package co.lp.arch;

import com.codahale.metrics.Snapshot;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.function.Consumer;

/**
 *
 * @author eitanya
 */
public class TestUtils {
  
    static String getHistData(final Snapshot snapshot) {
        return "(75%):"+ snapshot.get75thPercentile()+
                " (95%):" + snapshot.get95thPercentile()+
                " (98%):" + snapshot.get98thPercentile()+
                " (99%):" + snapshot.get99thPercentile()+
                " (99.9%):" + snapshot.get999thPercentile();
    }

    static void spawnThreadWithNewChannel(ConnectionFactory factory, ConsumeWithEx<Channel> f) {
        new Thread(() -> {
            try {
                Connection conn = factory.newConnection();
                Channel chan = conn.createChannel();
                f.accept(chan);
                chan.close();
                conn.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).start();
    }

    public static interface ConsumeWithEx<T> {

        void accept(T t) throws Exception;
    }

    public static class StringPs {

        PrintStream ps;
        ByteArrayOutputStream baos;

        public StringPs() {
            baos = new ByteArrayOutputStream();
            ps = new PrintStream(baos);
        }

        public PrintStream getPs() {
            return ps;
        }

        public String getString() {
            return baos.toString();
        }

    }  
    
    public static DefaultConsumer genConsumer(Channel chan, Consumer<byte[]> c) {
        return new DefaultConsumer(chan) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                c.accept(body);
            }
            
        };
    }
}
