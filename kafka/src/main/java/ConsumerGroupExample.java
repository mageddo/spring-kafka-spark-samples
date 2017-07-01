import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;

import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerGroupExample {
	private String a_zookeeper;
	private String a_groupId;
	private final String topic;
	private ExecutorService executor;
	private String kafkaServer;

	public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic, String kafkaServer) {
		this.a_zookeeper = a_zookeeper;
		this.a_groupId = a_groupId;
		this.topic = a_topic;
		this.kafkaServer = kafkaServer;
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
				ConsumerGroupExample.createConsumerConfig(a_zookeeper, a_groupId, kafkaServer));
			consumer.subscribe(Arrays.asList(this.topic));
//            consumer.seekToBeginning(consumer.assignment());
			executor.submit(new ConsumerTest(consumer, threadNumber));
			threadNumber++;

		}
	}

	private static Properties createConsumerConfig(String a_zookeeper, String a_groupId, String kafkaServer) {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaServer);
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

	public static void main(String[] args) throws UnknownHostException, InterruptedException {

		String zooKeeper = "zookeeper.dev:2181";
		String kafka = "kafka.dev:9092";
		String groupId = "ping.a";
		String topic = "Ping";
		int threads = 20;

		// Topic creation
		//
		final int portIndex = zooKeeper.indexOf(":");
		String address = zooKeeper.substring(0, portIndex);
		final String completeAddress = InetAddress.getByName(address).getHostAddress() + zooKeeper.substring(portIndex, zooKeeper.length());

		ZkClient client = new ZkClient(completeAddress, 10000, 10000, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(client, new ZkConnection(completeAddress), false);

//        if(AdminUtils.topicExists(zkUtils, topic)){
//            AdminUtils.deleteTopic(zkUtils, topic);
//            AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic);
////            AdminUtils.deleteConsumerGroupInZK(zkUtils, groupId);
//        }
//        try{
//            AdminUtils.createTopic(zkUtils, topic, 10, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
//        }catch (Exception e){
//            e.printStackTrace();
//        }

		// Consumption creation
		//
		ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic, kafka);
		example.run(threads);

		Thread.sleep(3000);
		// Producer creation
		//
		new ProducerExample(kafka, topic);

		try {
			Thread.sleep(1000000);
		} catch (InterruptedException ie) {

		}
		example.shutdown();
	}
}