package com.mageddo.kafka.message;

public interface KafkaUtils {

	static String nextTopic(String topic){
		return nextTopic(topic, 1);
	}
	static String nextTopic(String topic, int retriesTopics){
		final int i = topic.toUpperCase().indexOf("_RETRY");
		if(i >= 0){
			return getDLQ(topic.substring(0, i));
		}
		return topic + "_RETRY";
	}

	static String getDLQ(String topic){
		return String.format("%s_DLQ", topic);
	}
}
