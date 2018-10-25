package com.mageddo.kafka.poc;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;



public class StreamConsumerGroupExample {

	private final String zookeeper;
	private final String groupId;
	private final String topic;
	private ExecutorService executor;

	public StreamConsumerGroupExample(String zookeeper, String groupId, String a_topic) {
		this.zookeeper = zookeeper;
		this.groupId = groupId;
		this.topic = a_topic;
	}

	public void shutdown() {
		if (executor != null) executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int consumers) {
		final KafkaConsumer consumer = new KafkaConsumer<>(createConsumerConfig(zookeeper, groupId));
		executor = Executors.newFixedThreadPool(consumers);
		for (int i = 0; i < consumers; i++) {
			consumer.subscribe(Collections.singletonList(topic));
			executor.submit(new StreamConsumerTest(consumer, i));
		}
	}

	private static Map<String, Object> createConsumerConfig(String a_zookeeper, String a_groupId) {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return props;
	}

	public static void main(String[] args) {
		String zooKeeper = "zookeeper.dev:2181";
		String groupId = "ping.a";
		String topic = "Ping";
		int threads = 2;

		StreamConsumerGroupExample example = new StreamConsumerGroupExample(zooKeeper, groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(1000000);
		} catch (InterruptedException ie) {}
		example.shutdown();
	}
}
