//package com.mageddo.kafka.config;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.IntegerDeserializer;
//import org.apache.kafka.common.serialization.IntegerSerializer;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.*;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static com.mageddo.kafka.Application.SERVER;
//
///**
// * Created by elvis on 28/06/17.
// */
//@Configuration
//@EnableKafka
//@EnableAutoConfiguration
//@SpringBootApplication
//public class AutoCommitConfig {
//
//	public static final String PING_TEMPLATE = "PingTemplate";
//
//	@Value("${spring.kafka.bootstrap-servers}")
//	private String kafkaServer;
//
//	@Bean
//	ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
//			new ConcurrentKafkaListenerContainerFactory<>();
//		factory.setConsumerFactory(consumerFactory());
//		factory.setConcurrency(3);
//		return factory;
//	}
//
//	@Bean
//	public ConsumerFactory<Integer, String> consumerFactory() {
//		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
//	}
//
//	@Bean
//	public Map<String, Object> consumerConfigs() {
//		Map<String, Object> props = new HashMap<>();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group.a");
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		return props;
//	}
//
//	@Bean
//	public ProducerFactory<Integer, String> producerFactory() {
//		return new DefaultKafkaProducerFactory<>(producerConfigs());
//	}
//
//	@Bean
//	public Map<String, Object> producerConfigs() {
//		Map<String, Object> props = new HashMap<>();
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//		props.put(ProducerConfig.RETRIES_CONFIG, 0);
//		props.put(ProducerConfig.RETRIES_CONFIG, 0);
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		return props;
//	}
//
//	@Bean(PING_TEMPLATE)
//	public KafkaTemplate<Integer, String> kafkaTemplate() {
//		return new KafkaTemplate<>(producerFactory());
//	}
//}
