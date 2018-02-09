package com.mageddo.kafka.message;

import static com.mageddo.kafka.message.QueueEnum.Constants.*;

public enum QueueEnum {

	COLOR(Constants.COLOR, COLOR_FACTORY, 5, 5000, 3, false),
	EMAIL(Constants.EMAIL, EMAIL_FACTORY, 5, 5000, 3, true);

	private final String topic;
	private final String factory;
	private final int consumers;
	private final int interval;
	private final int tries;
	private final boolean autoConfigure;

	QueueEnum(String topic, String factory, int consumers, int interval, int tries, boolean autoConfigure) {
		this.topic = topic;
		this.factory = factory;
		this.consumers = consumers;
		this.interval = interval;
		this.tries = tries;
		this.autoConfigure = autoConfigure;
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

	public int getInterval() {
		return interval;
	}

	public int getTries() {
		return tries;
	}

	public boolean isAutoConfigure() {
		return autoConfigure;
	}

	public static class Constants {
		public static final String COLOR = "COLOR";
		public static final String COLOR_FACTORY = "COLOR_FACTORY";
		public static final String EMAIL = "EMAIL";
		public static final String EMAIL_FACTORY = "EMAIL_FACTORY";
	}


}
