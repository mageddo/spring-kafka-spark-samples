package com.mageddo.kafka.message;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConsumerDeclarer {

	@Autowired
	private ConfigurableBeanFactory beanFactory;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Value("${spring.kafka.consumer.autostartup:true}")
	private boolean autostartup;

	public void declare(final TopicDefinition ... topics) {
		for (TopicDefinition topic : topics) {
			declareConsumer(topic);
		}
	}

	public void declareConsumer(final TopicDefinition topic) {

		if(!topic.isAutoConfigure()){
			return ;
		}

		final ConcurrentKafkaListenerContainerFactory factory = new RetryableKafkaListenerContainerFactory();
		final boolean autoStartup = topic.getConsumers() > 0 && autostartup;
		if(autoStartup){
			factory.setConcurrency(topic.getConsumers());
		}
		factory.setAutoStartup(autoStartup);
//		factory.getContainerProperties().setAckOnError(false);
		factory.getContainerProperties().setAckMode(topic.getAckMode());
		factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()));

		final ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(topic.getInterval());
		policy.setMultiplier(1.0);
		policy.setMaxInterval(topic.getInterval());

		final SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(topic.getMaxTries());

		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setBackOffPolicy(policy);
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setThrowLastExceptionOnExhausted(true);
		factory.setRetryTemplate(retryTemplate);
		beanFactory.registerSingleton(topic.getFactory(), factory);
	}
}
