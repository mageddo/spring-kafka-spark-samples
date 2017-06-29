package com.mageddo.kafka;

import com.mageddo.kafka.config.AutoCommitConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mageddo.kafka.config.AutoCommitConfig.PING_TEMPLATE;

/**
 * Created by elvis on 26/06/17.
 */

@SpringBootTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {AutoCommitConfig.class})
public class AutoCommitTest {

	public static final String PING = "Ping";

	@ClassRule
	public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, PING);

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private CountDownLatch countDownLatch = new CountDownLatch(4);

	@Autowired
	@Qualifier(PING_TEMPLATE)
	private KafkaTemplate<Integer, String> template;

	@BeforeClass
	public static void setup() {
		System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
		System.setProperty("kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getZookeeperConnectionString());
	}

	@Test
	public void testMessageGroupByKey() throws Exception {

		this.template.send(PING, "foo9");
//		this.template.send(PING, new Integer(1), "foo1");
//		this.template.send(PING, new Integer(2), "foo2");
//		this.template.send(PING, new Integer(3), "foo3");
//		this.template.send(PING, new Integer(4), "foo4");

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
