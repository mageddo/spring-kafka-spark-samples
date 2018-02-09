package com.mageddo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.retry.*;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.ThreadWaitSleeper;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


@EnableKafka
@EnableScheduling
//@EnableAspectJAutoProxy
@EnableAutoConfiguration

@SpringBootApplication
@Configuration
public class Application {


	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private KafkaTemplate kafkaTemplate;

	private AtomicInteger atomicInteger = new AtomicInteger(0);

	@Scheduled(fixedDelay = 10_000)
	public void schedule() throws ExecutionException, InterruptedException {
		logger.info("m=schedule, status=posting");
		try {
			kafkaTemplate.send("COLOR_TOPIC", String.valueOf(atomicInteger.incrementAndGet())).get();
		} catch (Exception e){
			logger.error("status=post-error", e);
		}
	}

	@KafkaListener(containerFactory = "colorTopicFactory", topics = "COLOR_TOPIC"
//		,errorHandler = "myHandler"
	)
//	public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment){
	public void consume(ConsumerRecord<String, String> record){
//		new Random().nextBoolean()
		if(false){
			logger.info("status=consume-ok, offset={}, record={}", record.offset(), record.value());
//			acknowledgment.acknowledge();
		}else{
			logger.warn("status=consume-failed, offset={}, record={}", record.offset(), record.value());
			throw new RuntimeException("consume failed");
		}
	}

	@Bean
	public KafkaListenerErrorHandler myHandler(){
		return new KafkaListenerErrorHandler(){
			@Override
			public Object handleError(Message<?> message, ListenerExecutionFailedException exception) throws Exception {
				logger.info("status=error-handler >>>>>>>>>>");
				return null;
			}
		};
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> colorTopicFactory(){
		final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConcurrency(5);
		factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()));
		factory.getContainerProperties().setAckOnError(false);
//		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
//		factory.setRecoveryCallback(context -> {
//			logger.error("status=fatal", context.getLastThrowable());
//			return null;
//		});

		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.registerListener(new RetryListener() {
			@Override
			public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
				return true;
			}

			@Override
			public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {}

			@Override
			public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
				logger.info("status=retry");
				// apos cada falha do consumidor ele cai aqui
			}
		});

		ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(5000);
		policy.setMultiplier(1.0);
		policy.setMaxInterval(5000);
		retryTemplate.setBackOffPolicy(policy);

		final SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(3);
		retryTemplate.setRetryPolicy(retryPolicy);
//		factory.setRetryTemplate(retryTemplate);

		return factory;
	}


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

	public static void main(String[] args) {
		SpringApplication.run(Application.class);
	}
//
}
