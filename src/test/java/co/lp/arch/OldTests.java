package co.lp.arch;

import static co.lp.arch.TestUtils.*;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class OldTests {

    private static final String HOST_IP = System.getProperty("RMQ_IP", "192.168.33.10");
    ConnectionFactory factory;

    @Before
    public void setUp() {
        System.out.println("RMQ_IP is " + HOST_IP);
        factory = new ConnectionFactory();
        factory.setHost(HOST_IP);
    }

//    @Test
    public void queueOneMsg() throws InterruptedException {
        final String QUEUE_NAME = "hello";
        final String HELLO_WORLD = "Hello World!";
        ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);
        spawnThreadWithNewChannel(factory, chan -> {
            chan.queueDeclare(QUEUE_NAME, false, false, false, null);
            chan.basicPublish("", QUEUE_NAME, null, HELLO_WORLD.getBytes());
        });
        spawnThreadWithNewChannel(factory, chan -> {
            chan.queueDeclare(QUEUE_NAME, false, false, false, null);
            QueueingConsumer consumer = new QueueingConsumer(chan);
            chan.basicConsume(QUEUE_NAME, true, consumer);

            q.offer(new String(consumer.nextDelivery().getBody()));
        });
        assertEquals(HELLO_WORLD, q.poll(3, TimeUnit.SECONDS));
    }

//    @Test
    public void queueManyMsg() throws InterruptedException {
        final String LONG_Q = "long";
        final int NUM_OF_MSG = 100000;

        final MetricRegistry metrics = new MetricRegistry();
        final Histogram hist = metrics.histogram("hist");
        final Meter meter = metrics.meter("meter");
        final CountDownLatch latch = new CountDownLatch(1);
        final Semaphore semaphore = new Semaphore(1000);
        spawnThreadWithNewChannel(factory, chan -> {
            chan.queueDeclare(LONG_Q, false, false, false, null);
            for (int i = 0; i < NUM_OF_MSG; i++) {
                semaphore.acquire();
                chan.basicPublish("", LONG_Q, null, ByteBuffer.allocate(8).putLong(System.nanoTime()).array());
            }
        });
        spawnThreadWithNewChannel(factory, chan -> {
            chan.queueDeclare(LONG_Q, false, false, false, null);
            QueueingConsumer consumer = new QueueingConsumer(chan);
            chan.basicConsume(LONG_Q, true, consumer);
            for (int i = 0; i < NUM_OF_MSG; i++) {
                final long aLong = ByteBuffer.wrap(consumer.nextDelivery().getBody()).getLong();
                final long lat = (System.nanoTime() - aLong) / TimeUnit.MILLISECONDS.toNanos(1);
                semaphore.release();
                hist.update(lat);
                meter.mark();
            }
            latch.countDown();
        });
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        getHistData(hist.getSnapshot());
        System.out.println("rate " + meter.getMeanRate());
    }

//    @Test
    public void queueDirectMsg() throws InterruptedException {
        final String EXCHANGE_NAME = "queueDirectMsgEx";
        final String HELLO_WORLD = "Hello World!";
        final String ROUTING_KEY = "RK";
        ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);
        spawnThreadWithNewChannel(factory, chan -> {
            chan.exchangeDeclare(EXCHANGE_NAME, "direct");
            Thread.sleep(500);
            chan.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, HELLO_WORLD.getBytes());
        });
        spawnThreadWithNewChannel(factory, chan -> {
            chan.exchangeDeclare(EXCHANGE_NAME, "direct");
            String queueName = chan.queueDeclare().getQueue();
            chan.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);
//            QueueingConsumer consumer = new QueueingConsumer(chan);
            chan.basicConsume(queueName, true, genConsumer(chan, (body) -> q.offer(new String(body))));
            Thread.sleep(5000);
//            q.offer(new String(consumer.nextDelivery().getBody()));
        });
        assertEquals(HELLO_WORLD, q.poll(3, TimeUnit.SECONDS));
    }
}
