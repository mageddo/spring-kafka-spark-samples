package com.mageddo.kafka.message;

import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import java.util.Map;

public interface TopicDefinition {

	String getName();

	int getConsumers();

	String getFactory();

	long getInterval();

	int getMaxTries();

	boolean isAutoConfigure();

	AckMode getAckMode();

	Map<String, Object> getProps();
}
