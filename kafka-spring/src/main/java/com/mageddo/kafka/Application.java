package com.mageddo.kafka;

import com.mageddo.kafka.config.AutoCommitConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;


@EnableKafka
@EnableScheduling
//@EnableAspectJAutoProxy
@EnableAutoConfiguration

@SpringBootApplication
@Configuration
@Import(AutoCommitConfig.class)
public class Application { /*implements SchedulingConfigurer {*/
//
	public static final String SERVER = "kafka.dev:9092";
//	public final Logger logger = LoggerFactory.getLogger(getClass());
//
//	@Autowired
//	private KafkaTemplate<Integer, String> template;
//
//	private final CountDownLatch latch = new CountDownLatch(3);
//
//	@Override
//	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
//		taskRegistrar.setScheduler(Executors.newScheduledThreadPool(5));
//	}
//
//	@Scheduled(fixedDelay = 2000)
//	public void run() throws Exception {
//		this.template.send("myTopic", new Integer(1), "foo1");
//		this.template.send("myTopic", new Integer(1), "foo2");
//		this.template.send("myTopic", new Integer(1),"foo3");
//		logger.info("All received");
//	}
//
//	@KafkaListener(id = "foo", topics = "myTopic")
//	public void listen(ConsumerRecord<?, ?> cr) throws Exception {
// 		logger.info("status=received, msg={}", cr.toString());
////		latch.countDown();
//	}
//
//	public static void main(String[] args) {
//		SpringApplication.run(Application.class, args);
//	}
//
//	@Bean
//	ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
//			new ConcurrentKafkaListenerContainerFactory<>();
//		factory.setConsumerFactory(consumerFactory());
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
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		return props;
//	}
//
//
//	@Bean
//	public ProducerFactory<Integer, String> producerFactory() {
//		return new DefaultKafkaProducerFactory<>(producerConfigs());
//	}
//
//	@Bean
//	public Map<String, Object> producerConfigs() {
//		Map<String, Object> props = new HashMap<>();
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
//		props.put(ProducerConfig.RETRIES_CONFIG, 0);
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		return props;
//	}
//
//	@Bean
//	public KafkaTemplate<Integer, String> kafkaTemplate() {
//		return new KafkaTemplate<>(producerFactory());
//	}
//
}
