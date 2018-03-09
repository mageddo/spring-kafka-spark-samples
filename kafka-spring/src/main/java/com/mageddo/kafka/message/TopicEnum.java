package com.mageddo.kafka.message;

import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import java.time.Duration;
import java.util.Map;

import static com.mageddo.kafka.message.TopicEnum.Constants.*;

public enum TopicEnum implements TopicDefinition {

	COLOR(Constants.COLOR, COLOR_FACTORY, 5, 5000, 3, AckMode.RECORD, false),
	EMAIL(Constants.EMAIL, EMAIL_FACTORY, 0, 5000, 3, AckMode.RECORD, true),
	CHAT_MESSAGE(Constants.CHAT_MESSAGE, CHAT_MESSAGE_FACTORY, 1, Duration.ofSeconds(11).toMillis(), 3, AckMode.RECORD, true);

	private final String topic;
	private final String factory;
	private final int consumers;
	private final long interval;
	private final int maxTries;
	private final AckMode ackMode;
	private final boolean autoConfigure;
	private final Map<String, Object> props;
	private String dlq;

	TopicEnum(String topic, String factory, int consumers, long interval, int maxTries, AckMode ackMode, boolean autoConfigure) {
		this(topic, factory, consumers, interval, maxTries, ackMode, autoConfigure, MapBuilder.map());
	}

	TopicEnum(String topic, String factory, int consumers, long interval, int maxTries, AckMode ackMode, boolean autoConfigure, MapBuilder props) {
		this.topic = topic;
		this.factory = factory;
		this.consumers = consumers;
		this.interval = interval;
		this.maxTries = maxTries;
		this.ackMode = ackMode;
		this.autoConfigure = autoConfigure;
		this.props = props.get();
		this.dlq = String.format("%s_DLQ", getTopic());
	}

	public String getTopic() {
		return topic;
	}

	public String getFactory() {
		return factory;
	}

	public int getConsumers() {
		return consumers;
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

	@Override
	public AckMode getAckMode() {
		return ackMode;
	}

	@Override
	public Map<String, Object> getProps() {
		return props;
	}

	public String getDLQ() {
		return dlq;
	}

	public String retryTopic(){
		return String.format("%s_RETRY", getTopic());
	}

	public static class Constants {
		public static final String COLOR = "COLOR";
		public static final String COLOR_FACTORY = "COLOR_FACTORY";
		public static final String EMAIL = "EMAIL";
		public static final String EMAIL_FACTORY = "EMAIL_FACTORY";
		public static final String CHAT_MESSAGE = "CHAT_MESSAGE";
		public static final String CHAT_MESSAGE_FACTORY = "CHAT_MESSAGE_FACTORY";
	}

}
