package com.mageddo.kafka;

import com.mageddo.kafka.message.Topic;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;

import java.time.Duration;

import static com.mageddo.kafka.TopicEnum.Constants.*;

public enum TopicEnum  {

	COLOR(new Topic(Constants.COLOR, COLOR_FACTORY, 5, 5000, 3, AckMode.RECORD, false)),
	EMAIL(new Topic(Constants.EMAIL, EMAIL_FACTORY, 0, 5000, 3, AckMode.RECORD, true)),
	CHAT_MESSAGE(new Topic(Constants.CHAT_MESSAGE, CHAT_MESSAGE_FACTORY, 1, Duration.ofSeconds(11).toMillis(), 3, AckMode.RECORD, true));


	private final Topic topic;

	TopicEnum(Topic topic) {
		this.topic = topic;
	}

	public Topic getTopic() {
		return topic;
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
