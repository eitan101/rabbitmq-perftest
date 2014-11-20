package co.lp.arch;

import static co.lp.arch.TestUtils.*;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QueueJunits {

    @Parameterized.Parameters
    public static Collection primeNumbers() {
        return Arrays.asList(new Object[][]{
            {50000, 1},
            {50000, 10},
            {50000, 100},
            {50000, 1000},
            {50000, 10000},
            {50000, 50000},
        });
    }

    private static final String HOST_IP = System.getProperty("RMQ_IP", "127.0.0.1");
    private final int RK_NUM;
    private final int NUM_OF_MSG;
    ConnectionFactory factory;

    public QueueJunits(int NUM_OF_MSG, int Q_NUM) {
        this.RK_NUM = Q_NUM;
        this.NUM_OF_MSG = NUM_OF_MSG;
    }

    @Before
    public void setUp() {
//        System.out.println("RMQ_IP is " + HOST_IP);
        factory = new ConnectionFactory();
        factory.setHost(HOST_IP);
    }

    @Test
    public void queueManyQueuesAsync() throws InterruptedException {

        final String MY_EXCHANGE = "queueManyQueuesEx";
        final String ROUTING_KEY = "RK";
        final MetricRegistry metrics = new MetricRegistry();
        final Histogram latancyHistogram = metrics.histogram("hist");
        final Meter capacityMeter = metrics.meter("meter");
        final CountDownLatch allMsgRecieved = new CountDownLatch(NUM_OF_MSG);
        final CountDownLatch allConsumersUp = new CountDownLatch(RK_NUM);
        final Semaphore maxMsgsInQs = new Semaphore(1000);
        final Random rand = new Random();

        spawnThreadWithNewChannel(factory, chan -> {
            chan.exchangeDeclare(MY_EXCHANGE, "direct");
            allConsumersUp.await();
            for (int i = 0; i < NUM_OF_MSG; i++) {
                maxMsgsInQs.acquire();
                chan.basicPublish(MY_EXCHANGE, ROUTING_KEY + rand.nextInt(RK_NUM), null, ByteBuffer.allocate(8).putLong(System.nanoTime()).array());
            }
        });

        spawnThreadWithNewChannel(factory, chan -> {
            chan.exchangeDeclare(MY_EXCHANGE, "direct");
            final com.rabbitmq.client.Consumer consumer = genConsumer(chan, body -> {
                final long aLong = ByteBuffer.wrap(body).getLong();
                maxMsgsInQs.release();
                latancyHistogram.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - aLong));
                capacityMeter.mark();
                allMsgRecieved.countDown();
            });

            String queueName = chan.queueDeclare().getQueue();
            for (int i = 0; i < RK_NUM; i++) {
                chan.queueBind(queueName, MY_EXCHANGE, ROUTING_KEY + i);
                allConsumersUp.countDown();
            }
            chan.basicConsume(queueName, true, consumer);
            allMsgRecieved.await();
        });

        allConsumersUp.await();
        long tc = Thread.getAllStackTraces().keySet().stream().count();
        assertTrue(allMsgRecieved.await(Math.max(20, NUM_OF_MSG / 3000), TimeUnit.SECONDS));
        System.out.println("MSGS:" + NUM_OF_MSG + " "
                + "TOPICS:" + RK_NUM + " "
                + "CAPACITY(msg/s): " + capacityMeter.getMeanRate() + " "
                + "LAT(ms): " + getHistData(latancyHistogram.getSnapshot()) + " "
                + "Threads: " + tc);
    }
}
