import com.mageddo.kafka.poc.ProducerExample;
import com.mageddo.kafka.utils.KafkaEmbedded;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by elvis on 01/07/17.
 */
public class ConsumerGroupTest {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupTest.class);
	private static final String PING_TOPIC = "Ping";
	private static final String PING_GROUP_ID_A = "Ping.a";
	private static final String PING_GROUP_ID_B = "Ping.b";
	private static final int EXPECTED_REGISTERS = 5;
	private static final int CONSUMERS_QUANTTITY = 2;

	private final CountDownLatch aCountDownLatch = new CountDownLatch(EXPECTED_REGISTERS);
	private final Map<String, ConsumerRecord<String, String>> aRecords = new HashMap<>();

	private final CountDownLatch bCountDownLatch = new CountDownLatch(EXPECTED_REGISTERS);
	private final Map<String, ConsumerRecord<String, String>> bRecords = new HashMap<>();

	@ClassRule
	public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true, CONSUMERS_QUANTTITY, PING_TOPIC);

	@Test
	public void testPostAndconsume() throws Exception {

		final ExecutorService executorService = Executors.newFixedThreadPool(CONSUMERS_QUANTTITY);
		for (int threadNum = 0; threadNum < CONSUMERS_QUANTTITY; threadNum++) {
			executorService.submit(new SimpleConsumer(aRecords, aCountDownLatch, kafkaEmbedded, PING_TOPIC, PING_GROUP_ID_A, "a-" + threadNum));
			executorService.submit(new SimpleConsumer(bRecords, aCountDownLatch, kafkaEmbedded, PING_TOPIC, PING_GROUP_ID_B, "b-" + threadNum));
		}
		Thread.sleep(10_000);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(PING_TOPIC, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}

		executorService.shutdown();
		executorService.awaitTermination(EXPECTED_REGISTERS, TimeUnit.SECONDS);

		aCountDownLatch.await(10, TimeUnit.SECONDS);
		bCountDownLatch.await(10, TimeUnit.SECONDS);

		Assertions.assertThat(this.aRecords.size()).isEqualTo(EXPECTED_REGISTERS);
		Assertions.assertThat(this.aRecords.get("0x1388")).hasFieldOrPropertyWithValue("key", "1");

		Assertions.assertThat(this.bRecords.size()).isEqualTo(EXPECTED_REGISTERS);
		Assertions.assertThat(this.bRecords.get("0x1388")).hasFieldOrPropertyWithValue("key", "1");

	}

	private static Properties createConsumerConfig(String groupId, String kafkaServer, String zookeeper) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
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
