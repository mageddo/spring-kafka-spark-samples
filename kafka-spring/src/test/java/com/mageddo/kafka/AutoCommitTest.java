package com.mageddo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mageddo.kafka.Application.SERVER;
import static com.mageddo.kafka.config.AutoCommitConfig.PING_TEMPLATE;

/**
 * Created by elvis on 26/06/17.
 */

@SpringBootTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {Application.class})
public class AutoCommitTest {

	private static final String PING = "Ping";

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private CountDownLatch countDownLatch = new CountDownLatch(4);

	@Autowired
	@Qualifier(PING_TEMPLATE)
	private KafkaTemplate<Integer, String> template;

	@Test
	public void testMessageGroupByKey() throws Exception {

		this.template.send(PING, new Integer(1), "foo1");
		this.template.send(PING, new Integer(2), "foo2");
		this.template.send(PING, new Integer(3), "foo3");
		this.template.send(PING, new Integer(4), "foo4");

		Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));

	}

	@KafkaListener( /*topicPartitions = {
			@TopicPartition(topic = PING, partitions = {"0", "1", "2", "3"})
				//partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "0"))
		}*/ topics = PING)
	public void listen(ConsumerRecord<?, ?> cr) throws Exception {
		logger.info("status=received, msg={}", cr.toString());
		countDownLatch.countDown();
	}



}
