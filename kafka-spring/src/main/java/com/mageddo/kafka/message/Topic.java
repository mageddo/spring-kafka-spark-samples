package com.mageddo.kafka.message;

import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import java.util.HashMap;
import java.util.Map;

public class Topic implements TopicDefinition {

	private final String topic;
	private final String factory;
	private final int consumers;
	private final long interval;
	private final int maxTries;
	private final AckMode ackMode;
	private final boolean autoConfigure;
	private final Map<String, Object> props;

	public Topic(String topic, String factory, int consumers, long interval, int maxTries, AckMode ackMode, boolean autoConfigure) {
		this(topic, factory, consumers, interval, maxTries, ackMode, autoConfigure, null);
	}

	public Topic(String topic, String factory, int consumers, long interval, int maxTries, AckMode ackMode, boolean autoConfigure, MapBuilder props) {
		this.topic = topic;
		this.factory = factory;
		this.consumers = consumers;
		this.interval = interval;
		this.maxTries = maxTries;
		this.ackMode = ackMode;
		this.autoConfigure = autoConfigure;
		this.props = props == null ? null : props.get();
	}


	public String getName() {
		return topic;
	}

	public int getConsumers() {
		return consumers;
	}

	public String getFactory() {
		return factory;
	}

	public long getInterval() {
		return interval;
	}

	public int getMaxTries() {
		return maxTries;
	}

	public boolean isAutoConfigure() {
		return autoConfigure;
	}

	public AckMode getAckMode() {
		return ackMode;
	}

	public Map<String, Object> getProps() {
		return props;
	}

	public String retryTopic(){
		return KafkaUtils.nextTopic(getName());
	}

	public static class MapBuilder {
		private final Map<String, Object> map;

		public MapBuilder(Map<String, Object> map) {
			this.map = map;
		}

		public static MapBuilder map(){
			return new MapBuilder(new HashMap<>());
		}

		public MapBuilder prop(String k, Object v){
			this.map.put(k, v);
			return this;
		}

		public Map<String, Object> get(){
			return this.map;
		}
	}

	public static class Constants {

		public static final String SITE_PROFESSIONAL_FACTORY = "SITE_PROFESSIONAL_FACTORY";
	}

}
