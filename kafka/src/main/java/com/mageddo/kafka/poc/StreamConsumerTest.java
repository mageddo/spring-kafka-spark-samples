package com.mageddo.kafka.poc;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class StreamConsumerTest implements Runnable {
	private KafkaConsumer consumer;
	private int threadNumber;

	public StreamConsumerTest(KafkaConsumer consumer, int threadNumber) {
		this.threadNumber = threadNumber;
		this.consumer = consumer;
	}

	public void run() {
		while (!Thread.currentThread().isInterrupted()){
			final ConsumerRecords msgs = consumer.poll(2000);
			msgs.forEach(v -> {
				System.out.println("Thread " + threadNumber + ": " + v);

			});
		}
		System.out.println("Shutting down Thread: " + threadNumber);
	}

}
