package com.mageddo.kafka;

import com.mageddo.kafka.message.QueueEnum;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.concurrent.Executors;


@EnableKafka
@EnableScheduling
@EnableAspectJAutoProxy
@EnableAutoConfiguration

@SpringBootApplication
@Configuration
public class Application implements SchedulingConfigurer, InitializingBean {

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private ConfigurableBeanFactory beanFactory;

	public static void main(String[] args) {
		SpringApplication.run(Application.class);
	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setScheduler(Executors.newScheduledThreadPool(5));
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		for (QueueEnum queueEnum : QueueEnum.values()) {
			declareConsumer(queueEnum);
		}
	}

	private void declareConsumer(final QueueEnum queueEnum) {

		final ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConcurrency(queueEnum.getConsumers());
		factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties()));
//		factory.getContainerProperties().setAckOnError(false);
//		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
		factory.setRecoveryCallback(context -> {
			beanFactory.getBean(queueEnum.getTopic());
			return null;
		});

		final ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(queueEnum.getInterval());
		policy.setMultiplier(1.0);
		policy.setMaxInterval(queueEnum.getInterval());

		final SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(queueEnum.getTries());

		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setBackOffPolicy(policy);
		retryTemplate.setRetryPolicy(retryPolicy);
		factory.setRetryTemplate(retryTemplate);

	}
}
