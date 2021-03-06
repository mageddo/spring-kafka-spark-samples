package com.mageddo.kafka.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;

public class SimpleRetryListener implements org.springframework.retry.RetryListener {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
		return true;
	}

	@Override
	public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {

	}

	@Override
	public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
		logger.error("status=error, msg={}", throwable.getMessage(), throwable);
	}
}
