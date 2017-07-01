package com.mageddo.kafka.poc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest implements Runnable {
	private int m_threadNumber;
	private KafkaConsumer<String, Object> consumer;

	public ConsumerTest(KafkaConsumer<String, Object> consumer, int a_threadNumber) {
		this.consumer = consumer;
		m_threadNumber = a_threadNumber;
	}


	public void run() {
		while (true) {
			final ConsumerRecords<String, Object> records = consumer.poll(1000);
			for (ConsumerRecord<String, Object> record : records) {
				System.out.println("Thread " + m_threadNumber + ": " + record);
			}
		}
	}

}