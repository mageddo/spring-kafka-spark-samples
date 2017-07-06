package com.mageddo.kafka;

import com.mageddo.kafka.poc.ProducerExample;
import com.mageddo.kafka.utils.KafkaEmbedded;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by elvis on 01/07/17.
 */
public class ConsumerOffsetTest {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetTest.class);
	private static final String PING_TOPIC = "Ping";
	private static final String PING_GROUP_ID = "Ping.a";
	private static final int EXPECTED_REGISTERS = 5;
	private static final int CONSUMERS_QUANTTITY = 1;

	private final CountDownLatch countDownLatch = new CountDownLatch(EXPECTED_REGISTERS);
	private final ConcurrentMap<String, List<ConsumerRecord<String, String>>> records = new ConcurrentHashMap<>();

	@ClassRule
	public static KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1, true);

	public void declareTopics(int partitions, String ... topics){
		ZkUtils zkUtils = kafkaEmbedded.getZkUtils();
		Properties props = new Properties();
		for (String topic : topics) {
			AdminUtils.createTopic(zkUtils, topic, partitions, kafkaEmbedded.getCount(), props, null);
		}

	}

	@Test
	public void testPostAndconsumeFromBeginning() throws Exception {

		declareTopics(CONSUMERS_QUANTTITY, PING_TOPIC);

		final ExecutorService executorService = Executors.newFixedThreadPool(CONSUMERS_QUANTTITY);
		for (int threadNum = 0; threadNum < CONSUMERS_QUANTTITY; threadNum++) {

			final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
			final KafkaConsumer kafkaConsumer = new KafkaConsumer<>(conf);
			kafkaConsumer.subscribe(Arrays.asList(PING_TOPIC));

			executorService.submit(new SimpleOffsetConsumer(kafkaConsumer, records, countDownLatch, "a-" + threadNum));
		}

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(PING_TOPIC, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}

		executorService.shutdown();
		executorService.awaitTermination(EXPECTED_REGISTERS, TimeUnit.SECONDS);

		Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));

		Assertions.assertThat(this.records.size()).isEqualTo(2);

		final List<ConsumerRecord<String, String>> aRecordKeyOne = this.records.get("1");
		Assert.assertEquals(2, aRecordKeyOne.size());
		Assert.assertEquals(aRecordKeyOne.get(0).partition(), aRecordKeyOne.get(1).partition());

		final List<ConsumerRecord<String, String>> aRecordKeyTwo = this.records.get("2");
		Assert.assertEquals(3, this.records.get("2").size());
		Assert.assertEquals(2, aRecordKeyOne.size());

		Assertions.assertThat(aRecordKeyTwo.get(0).partition())
			.isEqualTo(aRecordKeyTwo.get(1).partition())
			.isEqualTo(aRecordKeyTwo.get(2).partition());

	}

	@Test
	public void testUsingSeek() throws Exception {

		declareTopics(CONSUMERS_QUANTTITY, PING_TOPIC);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(PING_TOPIC, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}
		final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(PING_TOPIC));

		final ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
		for (int i = 0; i < records.count(); i++) {
			countDownLatch.countDown();
		}
		Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
		Assert.assertEquals(0, kafkaConsumer.poll(1000).count());
		kafkaConsumer.seek(new TopicPartition(PING_TOPIC, records.iterator().next().partition()), 0);
		Assert.assertTrue(kafkaConsumer.poll(1000).count() > 0);

	}


	@Test
	public void testManualCommit() throws Exception {

		declareTopics(1, PING_TOPIC);

		int totalCount = 0;
		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(PING_TOPIC, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}
		final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		conf.remove(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(PING_TOPIC));

		ConsumerRecords<String, String> records;
		do {
			records = kafkaConsumer.poll(10000);
			for (ConsumerRecord<String, String> record : records) {

				if (new Random().nextBoolean()) {

					kafkaConsumer.commitSync(Collections.singletonMap(
						new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset())
					));
					countDownLatch.countDown();
				}else{
					// simulating error
					kafkaConsumer.seek(
						new TopicPartition(record.topic(), record.partition()), record.offset()
					);
					break;
				}
				totalCount++;
			}
		}while (!records.isEmpty());

		Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
		Assert.assertTrue(totalCount >= EXPECTED_REGISTERS);

	}

	private static Properties createConsumerConfig(String groupId, String kafkaServer) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return props;
	}

	static class SimpleOffsetConsumer implements Runnable {

		private final Logger logger = LoggerFactory.getLogger(getClass());
		private final ConcurrentMap<String, List<ConsumerRecord<String, String>>> records;
		private final CountDownLatch countDownLatch;
		private final String name;
		private final KafkaConsumer<String, String> consumer;

		SimpleOffsetConsumer(KafkaConsumer<String, String> consumer, ConcurrentMap<String,
				List<ConsumerRecord<String, String>>> records, CountDownLatch countDownLatch, String name) {

			this.consumer = consumer;
			this.records = records;
			this.countDownLatch = countDownLatch;
			this.name = name;
		}

		@Override
		public void run() {

			try {
				while (true) {
					final ConsumerRecords<String, String> records = consumer.poll(1000);
					for (final ConsumerRecord<String, String> record : records) {

						final List<ConsumerRecord<String, String>> value = new ArrayList<>();
						List<ConsumerRecord<String, String>> retVal = this.records.putIfAbsent(record.key(), value);
						if(retVal == null){
							retVal = value;
						}
						retVal.add(record);

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
