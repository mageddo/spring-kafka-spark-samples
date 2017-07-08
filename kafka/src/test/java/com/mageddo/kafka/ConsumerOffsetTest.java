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
import org.junit.Ignore;
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
	private static final String PING_TOPIC_V1 = "Pingv1";
	private static final String PING_TOPIC_V2 = "Pingv2";
	private static final String PING_TOPIC_V3 = "Pingv3";
	private static final String PING_TOPIC_V4 = "Pingv4";
	private static final String PING_GROUP_ID = "Ping.a";
	private static final String PING_GROUP_ID_V3 = "Ping.c";
	private static final int EXPECTED_REGISTERS = 5;
	private static final int CONSUMERS_QUANTTITY = 2;

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

		final String topic = PING_TOPIC_V2;

		declareTopics(CONSUMERS_QUANTTITY, topic);

		final ExecutorService executorService = Executors.newFixedThreadPool(CONSUMERS_QUANTTITY);
		for (int threadNum = 0; threadNum < CONSUMERS_QUANTTITY; threadNum++) {

			final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
			final KafkaConsumer kafkaConsumer = new KafkaConsumer<>(conf);
			kafkaConsumer.subscribe(Arrays.asList(topic));

			executorService.submit(new SimpleOffsetConsumer(kafkaConsumer, records, countDownLatch, "a-" + threadNum));
		}

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(topic, msgs[i][0], msgs[i][1]));
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

		final String topic = PING_TOPIC_V3;
		final String groupId = PING_GROUP_ID_V3;
		declareTopics(1, topic);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(topic, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}
		final Properties conf = createConsumerConfig(groupId, kafkaEmbedded.getBrokersAsString());
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));

		final ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
		for (int i = 0; i < records.count(); i++) {
			countDownLatch.countDown();
		}
		Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
		Assert.assertEquals(0, kafkaConsumer.poll(1000).count());
		kafkaConsumer.seek(new TopicPartition(topic, records.iterator().next().partition()), 0);
		Assert.assertTrue(kafkaConsumer.poll(1000).count() > 0);

	}


	@Test
	public void testManualCommit() throws Exception {

		final String topic = PING_TOPIC_V1;
		declareTopics(1, topic);

		int totalCount = 0;
		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		final String[][] msgs = {{"1", "0x1388"}, {"2", "0x2710"}, {"1", "0x4e20"}, {"2", "0x9c40"}, {"2", "0x13880"}, };
		for (int i = 0; i < msgs.length; i++) {
			producer.send(new ProducerRecord<>(topic, msgs[i][0], msgs[i][1]));
			logger.info("status=posted");

		}
		final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		conf.remove(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));

		ConsumerRecords<String, String> records;
		do {
			records = kafkaConsumer.poll(10000);
			for (ConsumerRecord<String, String> record : records) {

				if (new Random().nextBoolean()) {

					/**
					 * Esse commit soh serve para que caso a aplicacao morra a aplicacao vai trazer a partir do ultimo commitado
					 * e nao o poll inteiro novamente
					 */
					kafkaConsumer.commitSync(Collections.singletonMap(
						new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)
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

	/**
	 * When manual commit is set, the commit happen in next cases:
	 * 	when .seek() is called, when .commit() is called, or when the next .poll() is called, in this case it commits
	 * 	the last entire last .poll() call unless you called .seek() before
	 * @throws Exception
	 */
	@Test
	public void testManualCommitWhenConsumerClosesConnection() throws Exception {

		final String topic = PING_TOPIC_V4;
		declareTopics(1, topic);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		producer.send(new ProducerRecord<>(topic, "hi"));
		final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		conf.remove(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		Assert.assertEquals("First consume not success", 1, kafkaConsumer.poll(10000).count());
		kafkaConsumer.close();

		kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		Assert.assertEquals("Second consume not success", 1, kafkaConsumer.poll(10000).count());
	}

	/**
	 * Este teste mostra o que acontece se  se um consumidor estiver com auto commit e for morto (morte da JVM por exemplo)
	 * se outra instancia subir ela terá que esperar o tempo do SESSION_TIMEOUT_MS_CONFIG que é o tempo para o broker
	 * entender que o ultimo consumidor realmente morreu
	 *
	 * Para testar:
	 * Suba o kafka
	 * Rode o teste, deve printar um valor maior que zero
	 * Espere um tempo maior do que SESSION_TIMEOUT_MS_CONFIG e rode novamente, deve printar um valor maior do que zero novamente
	 * Espere um tempo MENOR do que SESSION_TIMEOUT_MS_CONFIG e rode novamente, o .poll vai esperar ateh bater o tempo do session e entao retornara os registros
	 */
	@Test
	@Ignore
	public void manualTestSessionTimeout(){
		String topic = "test";
		final String broker = "kafka.dev:9092";
		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(broker));
		producer.send(new ProducerRecord<>(topic, "hi"));
		final Properties conf = createConsumerConfig(PING_GROUP_ID, broker);
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		conf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 30000);
		conf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		conf.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");

		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		logger.info("results={}", kafkaConsumer.poll(40000).count());

	}


	/**
	 * Teste mostra que mesmo que o consumidor esteja vivo, se ele demorar mais do que o tempo especificado para fazer
	 * poll entao ele irá ser considerado morto pelo broker
	 * @throws Exception
	 */
	@Test
	public void testAutoCommitMeetingMaxPollTimeout() throws Exception {

		final String topic = PING_TOPIC_V4;
		declareTopics(1, topic);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerExample.config(kafkaEmbedded.getBrokersAsString()));
		producer.send(new ProducerRecord<>(topic, "hi"));
		final Properties conf = createConsumerConfig(PING_GROUP_ID, kafkaEmbedded.getBrokersAsString());
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		conf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

		/**
		 * Tempo que o kafka vai esperar o consumidor interagir antes de fazer rebalance
		 * Note que esse timeout soh vai ser respeitado se o consumidor estivesse vivo e a JVM fosse morta por exemplo
		 * caso o consumidor pare de consumir por alguma razao mas ainda esteja vivo entao o timeout a ser considerado
		 * será o MAX_POLL_INTERVAL_MS_CONFIG
		 */
//		conf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "40000");

		/**
		 * Intervalo de tempo em que o consumidor vai ficar pingando o broker dizendo que está vivo
		 */
//		conf.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "25000");

		/**
		 * Intervalo em que o broker vai esperar entre um .poll() e outro, caso passe do tempo especificado
		 * o broker vai considerar esse consumidor como morto e fazer rebalance, note que se esse timeout soh vai ser
		 * considerado se o consumidor estiver vivo e por alguma razao estiver demorando para fazer poll caso contrario
		 * o broker podera ter considerado o SESSION_TIMEOUT_MS_CONFIG e morrido antes
		 */
		conf.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		Assert.assertEquals("First consume not success", 1, kafkaConsumer.poll(10000).count());

		kafkaConsumer = new KafkaConsumer<>(conf);
		kafkaConsumer.subscribe(Arrays.asList(topic));
 		Assert.assertEquals("Second consume not success", 1, kafkaConsumer.poll(11000).count());
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
