package com.mageddo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mageddo.kafka.message.TopicEnum.Constants.EMAIL;
import static com.mageddo.kafka.message.TopicEnum.Constants.EMAIL_FACTORY;

@Component(EMAIL)
public class EmailConsumer implements RecoveryCallback<Object> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private KafkaTemplate kafkaTemplate;

	final AtomicInteger counter = new AtomicInteger(0);

//	@Scheduled(fixedDelay = 10_000)
	public void send() throws Exception {
			final String object = String.valueOf(counter.incrementAndGet());
		kafkaTemplate.send(new ProducerRecord<>(EMAIL, object)).get();
		logger.info("status=posted, counter={}", counter.get());
	}

	@KafkaListener(containerFactory = EMAIL_FACTORY, topics = EMAIL)
	public void consume(ConsumerRecord<String, String> record) throws InterruptedException {
		if(false){
			logger.info("status=consume-ok, offset={}, partition={}, key={}, record={}", record.offset(), record.partition(), record.key(), record.value());
//			acknowledgment.acknowledge();
			Thread.sleep(5000);
		}else{
			logger.warn("status=consume-failed, offset={}, record={}", record.offset(), record.value());
			throw new RuntimeException("consume failed");
		}
	}

	@Override
	public Object recover(RetryContext context) throws Exception {
		logger.error("status=recovered", context.getLastThrowable());
		kafkaTemplate.send(EMAIL + ".DLQ", ((ConsumerRecord)context.getAttribute("record")).value()).get();
		return null;
	}

}
