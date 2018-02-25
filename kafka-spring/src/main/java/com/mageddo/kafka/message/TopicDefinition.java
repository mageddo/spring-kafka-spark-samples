package com.mageddo.kafka.message;

import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import java.util.HashMap;
import java.util.Map;

public interface TopicDefinition {

	String getTopic();

	int getConsumers();

	String getFactory();

	long getInterval();

	int getMaxTries();

	boolean isAutoConfigure();

	AckMode getAckMode();

	Map<String, Object> getProps();

	class MapBuilder {
		private final Map<String, Object> map;

		public MapBuilder(Map<String, Object> map) {
			this.map = map;
		}

		public static MapBuilder map() {
			return new MapBuilder(new HashMap<>());
		}

		public MapBuilder prop(String k, Object v) {
			this.map.put(k, v);
			return this;
		}

		public Map<String, Object> get() {
			return this.map;
		}
	}
}
