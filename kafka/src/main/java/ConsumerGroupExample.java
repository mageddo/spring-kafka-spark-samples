import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerGroupExample {
    private String a_zookeeper;
    private String a_groupId;
    private final String topic;
    private ExecutorService executor;

    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic) {
        this.a_zookeeper = a_zookeeper;
        this.a_groupId = a_groupId;
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void run(int a_numThreads) {
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (int i = 0; i < a_numThreads; i++) {
            final KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(
                    ConsumerGroupExample.createConsumerConfig(a_zookeeper, a_groupId));
            consumer.subscribe(Arrays.asList(this.topic));
            executor.submit(new ConsumerTest(consumer, threadNumber));
            threadNumber++;

        }
    }
 
    private static Properties createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka.dev:9092");
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
 
        return props;
    }
 
    public static void main(String[] args) {

        String zooKeeper = "zookeeper.dev:2181";
        String groupId = "ping.a";
        String topic = "Ping";
        int threads = 2;
 
        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        example.run(threads);
 
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException ie) {
 
        }
        example.shutdown();
    }
}