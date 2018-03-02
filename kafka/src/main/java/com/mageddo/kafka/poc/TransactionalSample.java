package com.mageddo.kafka.poc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;

public class TransactionalSample {

	public static void main(String[] args) throws InterruptedException {

		final String broker = "zookeeper.intranet:9092";
		Executors.newFixedThreadPool(1).execute(() -> {
			final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig(broker));
//			consumer.seekToEnd();
			try {
				consumer.subscribe(Collections.singleton("MyTopic"));
				while (true) {
					ConsumerRecords<Object, Object> poll = consumer.poll(1000);
					for (ConsumerRecord<Object, Object> record : poll) {
						System.out.printf("consumed, record=%s%n", record.value());
					}
				}
			} finally {
				consumer.close();
			}
		});

		final KafkaProducer<String, Object> producer = new KafkaProducer<>(config(broker));
		int i = 1;
		producer.initTransactions();
		while (i == 1) {

			final ProducerRecord<String, Object> record = new ProducerRecord<>(
				"MyTopic", String.valueOf(new Random().nextInt(3) + 1), Integer.toHexString(new Random().nextInt(100))
			);
			producer.beginTransaction();
			producer.send(record);
			System.out.println("sending, record=" + record.value());
			producer.commitTransaction();
//			producer.flush();
			System.out.println("sent, record=" + record.value());
			Thread.sleep(1000);
			System.out.printf("");
		}
		producer.close();
	}

	private static Properties consumerConfig(String server) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	public static Map<String, Object> config(String kafkaServer) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
		props.put("request.required.acks", "1");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ProducerConfig.RETRIES_CONFIG, 1);

		return props;
	}
}
