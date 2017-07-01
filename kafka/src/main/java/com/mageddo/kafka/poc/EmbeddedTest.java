package com.mageddo.kafka.poc;

import com.mageddo.kafka.utils.KafkaEmbedded;
import org.junit.Test;

/**
 * Created by elvis on 01/07/17.
 */

//@RunWith()
public class EmbeddedTest {

	@Test
	public void testStartEmbedded() throws Exception {
		final KafkaEmbedded kafkaEmbedded = new KafkaEmbedded(1);
		kafkaEmbedded.before();
		System.out.println(kafkaEmbedded.getBrokerAddresses());
	}
}
