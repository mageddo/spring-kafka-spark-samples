package com.mageddo.kafka.poc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TransactionalSample {

	public static void main(String[] args) {
		final KafkaProducer<String, Object> producer = new KafkaProducer<>(config("kafka.dev:9092"));
		int i = 1;
		producer.initTransactions();
		while (i == 1) {

			final ProducerRecord<String, Object> record = new ProducerRecord<>(
				"MyTopic", String.valueOf(new Random().nextInt(3) + 1), Integer.toHexString(new Random().nextInt(100))
			);
			producer.beginTransaction();
			producer.send(record);
			producer.commitTransaction();
//			producer.flush();
			System.out.println("posted");

		}
		producer.close();
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
