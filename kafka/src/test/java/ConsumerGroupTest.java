import com.mageddo.kafka.poc.ProducerExample;
import com.mageddo.kafka.utils.KafkaEmbedded;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by elvis on 01/07/17.
 */
public class ConsumerGroupTest {

	private static final String PING_TOPIC = "Ping2";
	private static final String PING_GROUP_ID = "Ping.a1";
	private static final int CONSUMERS_QUANTTITY = 20;
	private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupTest.class);

	private final Map<String, ConsumerRecord<String, String>> records = new TreeMap<>();
	private final CountDownLatch countDownLatch = new CountDownLatch(5);

//	@ClassRule
//	public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true, 1, PING_TOPIC);
	public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1);


	@Test
	public void testPostAndconsume() throws Exception {

		kafkaEmbedded.before();

		ZkClient client = new ZkClient(kafkaEmbedded.getZookeeperConnectionString(), 10000, 10000, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(client, new ZkConnection(kafkaEmbedded.getZookeeperConnectionString()), false);

//        if(AdminUtils.topicExists(zkUtils, topic)){
//            AdminUtils.deleteTopic(zkUtils, topic);
//            AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic);
////            AdminUtils.deleteConsumerGroupInZK(zkUtils, groupId);
//        }
		try{
			AdminUtils.createTopic(zkUtils, PING_TOPIC, 10, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
		}catch (Exception e){
			e.printStackTrace();
		}

		logger.info("status=posted");
		final ExecutorService executorService = Executors.newFixedThreadPool(CONSUMERS_QUANTTITY);
		for (int threadNum = 0; threadNum < CONSUMERS_QUANTTITY; threadNum++) {
			executorService.submit(new SimpleConsumer(records, countDownLatch, kafkaEmbedded, PING_TOPIC, PING_GROUP_ID, String.valueOf(threadNum)));
		}

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
//		for (int i = 0; i < msgs.length; i++) {
		for(;;){
//			final ProducerRecord<String, String> record = new ProducerRecord<>(PING_TOPIC, msgs[i][0], msgs[i][1]);
//			producer.send(record);
			producer.send(new ProducerRecord<>(PING_TOPIC, "x" ));
			producer.flush();
			logger.info("status=posted");
			Thread.sleep(1000);

		}

//		countDownLatch.await(10, TimeUnit.SECONDS);
//
//		executorService.shutdown();
//		executorService.awaitTermination(5, TimeUnit.SECONDS);
	}

	private static Properties createConsumerConfig(String groupId, String kafkaServer, String zookeeper) {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaServer);
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		props.put("enable.auto.commit", true);
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
//		Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//		props.put("zookeeper.connect", zookeeper);
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
//
//		props.put("key.deserializer", StringDeserializer.class);
//		props.put("value.deserializer", StringDeserializer.class);

		return props;
	}

	static class SimpleConsumer implements Runnable {
		private final Logger logger = LoggerFactory.getLogger(getClass());
		private final Map<String, ConsumerRecord<String, String>> records;
		private final CountDownLatch countDownLatch;
		private final KafkaEmbedded kafkaEmbedded;
		private final String name;
		private final String groupId;
		private final String topic;

		SimpleConsumer(Map<String, ConsumerRecord<String, String>> records, CountDownLatch countDownLatch,
									 KafkaEmbedded kafkaEmbedded, String topic, String groupId, String name) {
			this.records = records;
			this.countDownLatch = countDownLatch;
			this.kafkaEmbedded = kafkaEmbedded;
			this.name = name;
			this.groupId = groupId;
			this.topic = topic;
		}

		@Override
		public void run() {

			try {
				final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
					createConsumerConfig(groupId, kafkaEmbedded.getBrokersAsString(), kafkaEmbedded.getZookeeperConnectionString())
				);
				consumer.subscribe(Arrays.asList(topic));
				while (true) {
					final ConsumerRecords<String, String> records = consumer.poll(1000);
					for (final ConsumerRecord<String, String> record : records) {
						this.records.put(record.value(), record);
						countDownLatch.countDown();
						logger.info("thread={}, record={}", name, record);
					}
				}
			} catch (Exception e) {
				logger.error("msg={}", e.getMessage(), e);
			}
		}
	}
}
