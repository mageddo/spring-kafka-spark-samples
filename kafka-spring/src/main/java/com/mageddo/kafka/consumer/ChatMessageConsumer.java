package com.mageddo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mageddo.kafka.message.TopicEnum.Constants.CHAT_MESSAGE;
import static com.mageddo.kafka.message.TopicEnum.Constants.CHAT_MESSAGE_FACTORY;

/**
 * Este consumidor prova que a configuracao do ConsumerDeclarer está funcionando assim, quando
 * as tentativas esgotarem o consumidor respectivo será chamado
 *
 * Este consumidor tambem prova que por mais que o intervalo de retentativa seja maior que o session.timeout.ms, o
 * kafka está fazendo hearbeat em background, sendo que o max.poll.interval.ms está para Integer.MAX_VALUE porém, é seguro
 * pois se o consumidor cair nao fará heartbeat e entao o kafka fará rebalanceamento
 *
 * Com estas configuracoes nao tive nenhum tipo de output de log do kafka
 */
@Component
public class ChatMessageConsumer implements RecoveryCallback<Object> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final KafkaTemplate kafkaTemplate;
	private AtomicInteger counter = new AtomicInteger(0);

	public ChatMessageConsumer(KafkaTemplate kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Scheduled(fixedDelay = 2_000)
	public void send() throws Exception {
		final String object = String.valueOf(counter.incrementAndGet());
		kafkaTemplate.send(new ProducerRecord<>(CHAT_MESSAGE, object)).get();
		logger.info("status=posted, counter={}", counter.get());
	}

	@KafkaListener(containerFactory = CHAT_MESSAGE_FACTORY, topics = CHAT_MESSAGE)
	public void consume(ConsumerRecord<String, String> record) throws Exception {
		Thread.sleep(Duration.ofSeconds(12).toMillis());
		if(new Random().nextBoolean()){
			logger.info("status=consumed, msg={}, partition={}, offset={}", record.value(), record.partition(), record.offset());
		} else{
			logger.info("status=failed, msg={}, partition={}, offset={}", record.value(), record.partition(), record.offset());
			throw new  RuntimeException("consume failed");
		}
	}

	@Override
	public Object recover(RetryContext context) throws Exception {
		logger.error("status=fatal");
		return null;
	}

}