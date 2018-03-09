package com.mageddo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mageddo.kafka.TopicEnum.Constants.COLOR;
import static com.mageddo.kafka.TopicEnum.Constants.COLOR_FACTORY;

@Component(COLOR)
public class ColorConsumer implements RecoveryCallback<Object> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private KafkaTemplate kafkaTemplate;

	final AtomicInteger counter = new AtomicInteger(0);

	public void send(){
		kafkaTemplate.send(COLOR, String.valueOf(counter.incrementAndGet()));
		logger.info("status=posted, counter={}", counter.get());
	}

	@KafkaListener(containerFactory = COLOR_FACTORY, topics = COLOR  /*,errorHandler = "myHandler" */ )
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

	@Override
	public Object recover(RetryContext context) throws Exception {
		logger.error("status=fatal", context.getLastThrowable());
		return null;
	}

	@Bean(COLOR_FACTORY)
	public ConcurrentKafkaListenerContainerFactory factory(){
		final ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConcurrency(5);
		factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()));
//		factory.getContainerProperties().setAckOnError(false);
//		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
		factory.setRecoveryCallback(ColorConsumer.this);

		final ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(5000);
		policy.setMultiplier(1.0);
		policy.setMaxInterval(5000);

		final SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(3);

		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setBackOffPolicy(policy);
		retryTemplate.setRetryPolicy(retryPolicy);
		factory.setRetryTemplate(retryTemplate);

		return factory;
	}

	@Bean
	public KafkaListenerErrorHandler colorHandler(){
		return new KafkaListenerErrorHandler(){
			@Override
			public Object handleError(Message<?> message, ListenerExecutionFailedException exception) throws Exception {
				logger.info("status=error-handler >>>>>>>>>>");
				return null;
			}
		};
	}

}
