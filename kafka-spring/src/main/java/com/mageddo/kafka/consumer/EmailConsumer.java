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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mageddo.kafka.message.QueueEnum.Constants.*;

@Component(EMAIL)
public class EmailConsumer implements RecoveryCallback<Object> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private KafkaTemplate kafkaTemplate;

	final AtomicInteger counter = new AtomicInteger(0);

	@Scheduled(fixedDelay = 10_000)
	public void send() throws Exception {
		kafkaTemplate.send(EMAIL, String.valueOf(counter.incrementAndGet())).get();
		logger.info("status=posted, counter={}", counter.get());
	}

	@KafkaListener(containerFactory = EMAIL_FACTORY, topics = EMAIL)
	public void consume(ConsumerRecord<String, String> record){
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
		logger.error("status=recovered", context.getLastThrowable());
		kafkaTemplate.send(EMAIL + ".DLQ", context.getAttribute("x")).get();
		return null;
	}

}
